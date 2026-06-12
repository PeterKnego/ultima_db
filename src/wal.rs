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
    /// Marker recording that a bulk load replaced `tables` at this entry's
    /// version. Bulk-loaded data itself is not WAL-logged; the marker lets
    /// recovery detect WAL commits that were made on top of a load no
    /// checkpoint covers (such commits cannot be replayed against pre-load
    /// state) and fail with [`Error::BulkLoadNotCheckpointed`] instead of
    /// silently producing a state no client ever observed.
    BulkLoad {
        tables: Vec<String>,
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
const TAG_BULK_LOAD: u8 = 6;

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
            WalOp::BulkLoad { tables } => {
                buf.push(TAG_BULK_LOAD);
                bincode::encode_into_std_write(tables, &mut buf, config)
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
            TAG_BULK_LOAD => {
                let (tables, read): (Vec<String>, _) =
                    bincode::decode_from_slice(&data[offset..], config)
                        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                ops.push(WalOp::BulkLoad { tables });
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

/// Frame one entry as the on-disk WAL record: `[len: u32 LE][bincode][crc32: u32 LE]`.
/// Shared by every `WalSink` so all backends produce a byte-identical format.
fn frame_entry(entry: &WalEntry) -> Result<Vec<u8>> {
    let data = serialize_entry(entry)?;
    let len = data.len() as u32;
    let checksum = crc32(&data);
    let mut buf = Vec::with_capacity(4 + data.len() + 4);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&data);
    buf.extend_from_slice(&checksum.to_le_bytes());
    Ok(buf)
}

fn write_entry_to_file(file: &mut File, entry: &WalEntry) -> Result<()> {
    file.write_all(&frame_entry(entry)?)
        .map_err(|e| Error::Persistence(e.to_string()))
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

        if len == 0 {
            // Clean end-of-log: a pre-sized (mmap) file leaves a zero tail after
            // a crash, and a torn zero-length write is not a real record (every
            // valid record frames a non-empty payload). Stop, do not error.
            break;
        }

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

/// Rewrite the WAL file, removing all entries with version <= `up_to_version`.
/// Returns `true` if a rewrite happened, `false` if there was nothing to prune.
///
/// Uses write-to-temp + atomic rename: a crash at any point leaves either the
/// complete old WAL or the complete pruned WAL (plus possibly a stray `.tmp`
/// that the next prune overwrites). The caller must be the only appender —
/// in production this runs *on* the WAL background thread (via
/// [`WalSink::prune`]), which reopens its append handle on the renamed file
/// afterwards. Rewriting concurrently with a live appender would destroy
/// acknowledged entries.
pub(crate) fn prune_wal(path: &Path, up_to_version: u64) -> Result<bool> {
    let entries = read_wal(path)?;
    let remaining: Vec<&WalEntry> = entries
        .iter()
        .filter(|e| e.version > up_to_version)
        .collect();

    if remaining.len() == entries.len() {
        return Ok(false); // nothing to prune
    }

    let mut buf = Vec::new();
    for entry in &remaining {
        buf.extend_from_slice(&frame_entry(entry)?);
    }

    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));
    let mut tmp = File::create(&tmp_path).map_err(|e| Error::Persistence(e.to_string()))?;
    tmp.write_all(&buf)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    tmp.sync_all()
        .map_err(|e| Error::Persistence(e.to_string()))?;
    drop(tmp);
    std::fs::rename(&tmp_path, path).map_err(|e| Error::Persistence(e.to_string()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(true)
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
    /// Remove entries with version <= `up_to_version` from the backing
    /// store. Runs on the WAL background thread between batches, so it is
    /// serialized with appends by construction. Sinks that rewrite the
    /// file must reopen their handle afterwards.
    fn prune(&mut self, _up_to_version: u64) -> Result<()> {
        Err(Error::Persistence(
            "prune not supported by this WAL sink".into(),
        ))
    }
}

/// Selects which `WalSink` implementation a `WalHandle` uses. `FsWrite` (the
/// default) and `Coalesced` are production-safe; the remaining variants are
/// experimental and bench-only.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalSinkKind {
    /// Baseline: one `write_all` per entry (the framed record) + `sync_all` per batch.
    FsWrite,
    /// Coalesced single `write` per batch + `sync_all` (fsync). Production-safe.
    Coalesced,
    /// Coalesced single `write` per batch + `sync_data` (fdatasync). Bench comparison only.
    BufferedFile,
    /// Pre-sized mmap sink (experimental, bench-only). memcpy into the mapped
    /// region; msync on flush; truncate to logical length on Drop.
    #[cfg(feature = "bench-internals")]
    Mmap,
    /// io_uring sink (experimental, bench-only, Linux). One `Write` + `Fsync(DATASYNC)`
    /// chained with `IO_LINK` per `sync` call; submitted in a single `io_uring_enter`.
    #[cfg(all(target_os = "linux", feature = "wal-iouring"))]
    IoUring,
}

/// Production sink: appends framed entries to a file and fsyncs it.
struct FileSink {
    file: File,
    path: std::path::PathBuf,
}

impl FileSink {
    /// Open (creating if needed) the WAL file in `dir` for appending.
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = open_wal_append(&wal_path)?;
        sync_dir(dir)?;
        Ok(FileSink {
            file,
            path: wal_path,
        })
    }
}

/// Open a WAL file for appending (creating it if needed).
fn open_wal_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| Error::Persistence(e.to_string()))
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
    fn prune(&mut self, up_to_version: u64) -> Result<()> {
        if prune_wal(&self.path, up_to_version)? {
            // The rewrite replaced the file via rename; reopen the append
            // handle so subsequent appends land in the new file, not the
            // old (unlinked) inode.
            self.file = open_wal_append(&self.path)?;
        }
        Ok(())
    }
}

/// Coalescing sink: `append` frames into an in-memory buffer (no syscall);
/// `sync` writes the whole batch in one `write` then fsyncs. Coalescing
/// (one `write` per batch) is identical regardless of the sync mode.
struct BufferedFileSink {
    file: File,
    path: std::path::PathBuf,
    buf: Vec<u8>,
    /// When true, `sync` uses `sync_data` (fdatasync); when false, `sync_all`
    /// (full fsync). Coalescing (one `write` per batch) is identical either way.
    datasync: bool,
}

impl BufferedFileSink {
    fn open(dir: &Path, datasync: bool) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = open_wal_append(&wal_path)?;
        sync_dir(dir)?;
        Ok(BufferedFileSink {
            file,
            path: wal_path,
            buf: Vec::new(),
            datasync,
        })
    }
}

impl WalSink for BufferedFileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf).map_err(|e| Error::Persistence(e.to_string()))?;
            self.buf.clear(); // retains capacity for the next batch
        }
        if self.datasync {
            self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))
        } else {
            self.file.sync_all().map_err(|e| Error::Persistence(e.to_string()))
        }
    }
    fn prune(&mut self, up_to_version: u64) -> Result<()> {
        // The bg thread always syncs a batch before pruning, so the buffer
        // is empty here; flush defensively in case that ever changes.
        if !self.buf.is_empty() {
            self.sync()?;
        }
        if prune_wal(&self.path, up_to_version)? {
            self.file = open_wal_append(&self.path)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MmapSink — experimental mmap-based WAL sink (bench-only, feature-gated)
// ---------------------------------------------------------------------------

/// Pre-sized mmap sink (experimental, bench-only). `append` memcpys framed bytes
/// into the mapped region at a tracked write head; `sync` `msync`s the map.
///
/// NOT safe with `prune_wal`/checkpoint (truncating the file under the mapping
/// risks SIGBUS). Assumes it opens an empty/clean file (the bench uses a fresh
/// dir per iteration). On clean `Drop` the file is truncated to the logical
/// write head; a crash leaves a zero tail that `read_wal` treats as end-of-log.
#[cfg(feature = "bench-internals")]
struct MmapSink {
    file: File,
    map: memmap2::MmapMut,
    write_head: usize,
    capacity: usize,
}

#[cfg(feature = "bench-internals")]
const MMAP_GROW_QUANTUM: u64 = 8 * 1024 * 1024;

#[cfg(feature = "bench-internals")]
impl MmapSink {
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        let existing = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        let capacity = ((existing / MMAP_GROW_QUANTUM) + 1) * MMAP_GROW_QUANTUM;
        file.set_len(capacity).map_err(|e| Error::Persistence(e.to_string()))?;
        // SAFETY: MmapSink owns `file` exclusively; no other handle aliases this
        // mapping, and (per this sink's bench-only contract) there are no concurrent
        // writers to the backing file.
        let map = unsafe { memmap2::MmapMut::map_mut(&file).map_err(|e| Error::Persistence(e.to_string()))? };
        Ok(MmapSink { file, map, write_head: existing as usize, capacity: capacity as usize })
    }

    /// Grow the file + remap if `extra` more bytes would not fit.
    fn ensure_capacity(&mut self, extra: usize) -> Result<()> {
        if self.write_head + extra <= self.capacity {
            return Ok(());
        }
        let needed = (self.write_head + extra) as u64;
        let new_cap = ((needed / MMAP_GROW_QUANTUM) + 1) * MMAP_GROW_QUANTUM;
        self.file.set_len(new_cap).map_err(|e| Error::Persistence(e.to_string()))?;
        // Persist the new size before remapping so a crash can't expose a hole.
        self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))?;
        // SAFETY: the old mapping is replaced atomically (the assignment drops
        // the previous MmapMut before the new one is established); `self.file`
        // is the sole owner of the backing fd, and no concurrent accessor holds
        // a reference into the old map at this point.
        self.map = unsafe { memmap2::MmapMut::map_mut(&self.file).map_err(|e| Error::Persistence(e.to_string()))? };
        self.capacity = new_cap as usize;
        Ok(())
    }
}

#[cfg(feature = "bench-internals")]
impl WalSink for MmapSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let framed = frame_entry(entry)?;
        self.ensure_capacity(framed.len())?;
        self.map[self.write_head..self.write_head + framed.len()].copy_from_slice(&framed);
        self.write_head += framed.len();
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        // MS_SYNC only the bytes actually written, not the whole pre-sized map.
        self.map
            .flush_range(0, self.write_head)
            .map_err(|e| Error::Persistence(e.to_string()))
    }
}

#[cfg(feature = "bench-internals")]
impl Drop for MmapSink {
    fn drop(&mut self) {
        let _ = self.map.flush();
        let _ = self.file.set_len(self.write_head as u64);
        let _ = self.file.sync_all();
    }
}

// ---------------------------------------------------------------------------
// IoUringSink — experimental io_uring-based WAL sink (Linux, wal-iouring feature)
// ---------------------------------------------------------------------------

/// io_uring sink (experimental, bench-only, Linux). `append` accumulates framed
/// bytes; `sync` submits one `Write` + `Fsync(DATASYNC)` chained with `IO_LINK`
/// in a single `io_uring_enter`, then waits on completion. Queue depth 8.
///
/// NOT safe with `prune_wal`/checkpoint. Writes at an explicit offset (not append
/// mode). Same on-disk format as the file sinks.
#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
struct IoUringSink {
    ring: io_uring::IoUring,
    file: File,
    offset: u64,
    buf: Vec<u8>,
}

#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
impl IoUringSink {
    fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let wal_path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        let offset = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        let ring = io_uring::IoUring::new(8).map_err(|e| Error::Persistence(e.to_string()))?;
        Ok(IoUringSink { ring, file, offset, buf: Vec::new() })
    }
}

#[cfg(all(target_os = "linux", feature = "wal-iouring"))]
impl WalSink for IoUringSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        use std::os::unix::io::AsRawFd;
        if self.buf.is_empty() {
            return Ok(());
        }
        debug_assert!(
            self.buf.len() <= u32::MAX as usize,
            "WAL batch exceeds io_uring single-write limit"
        );
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write_e = io_uring::opcode::Write::new(fd, self.buf.as_ptr(), self.buf.len() as u32)
            .offset(self.offset)
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK)
            .user_data(1);
        let fsync_e = io_uring::opcode::Fsync::new(fd)
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build()
            .user_data(2);
        {
            let mut sq = self.ring.submission();
            // Ring depth is 8 and we push exactly 2 entries per call, draining
            // the completion queue before returning, so the SQ is never full
            // here. Assert to catch any regression to that invariant.
            debug_assert!(!sq.is_full(), "io_uring submission queue unexpectedly full");
            // SAFETY: `self.buf` outlives the submission — `submit_and_wait`
            // below blocks until both ops complete, and we neither mutate nor
            // free `buf` until after the completions are reaped. `fd` is valid
            // for the lifetime of `self.file`.
            unsafe {
                sq.push(&write_e).map_err(|e| Error::Persistence(e.to_string()))?;
                sq.push(&fsync_e).map_err(|e| Error::Persistence(e.to_string()))?;
            }
        }
        // On a submit error the kernel may already have queued CQEs; drain them
        // so the ring is clean for the next call.
        self.ring.submit_and_wait(2).map_err(|e| {
            let _ = self.ring.completion().collect::<Vec<_>>();
            Error::Persistence(e.to_string())
        })?;

        // Reap both completions keyed by user_data (CQE order is not guaranteed
        // to match submission order, even with IO_LINK).
        let mut write_res: Option<i32> = None;
        let mut fsync_res: Option<i32> = None;
        for cqe in self.ring.completion() {
            match cqe.user_data() {
                1 => write_res = Some(cqe.result()),
                2 => fsync_res = Some(cqe.result()),
                other => {
                    return Err(Error::Persistence(format!(
                        "io_uring unexpected completion user_data={other}"
                    )));
                }
            }
        }

        let write_res =
            write_res.ok_or_else(|| Error::Persistence("io_uring missing write completion".into()))?;
        if write_res < 0 {
            return Err(Error::Persistence(format!(
                "io_uring write failed: {}",
                std::io::Error::from_raw_os_error(-write_res)
            )));
        }
        match fsync_res {
            None => return Err(Error::Persistence("io_uring missing fsync completion".into())),
            Some(r) if r < 0 => {
                return Err(Error::Persistence(format!(
                    "io_uring fsync failed: {}",
                    std::io::Error::from_raw_os_error(-r)
                )));
            }
            Some(_) => {}
        }
        if write_res as usize != self.buf.len() {
            return Err(Error::Persistence(format!(
                "io_uring short write: {} of {}",
                write_res,
                self.buf.len()
            )));
        }
        self.offset += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
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

/// Message processed by the WAL background thread: an entry to append, or a
/// prune request (executed between batches, serialized with appends).
pub(crate) enum WalMsg {
    Entry(WalEntry),
    Prune {
        up_to_version: u64,
        /// Completion ack; the requester blocks on the paired receiver.
        done: mpsc::Sender<Result<()>>,
    },
}

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
    sender: Option<mpsc::Sender<WalMsg>>,
    bg_thread: Option<thread::JoinHandle<()>>,
    consistent: bool,
    sync_state: Option<Arc<WalSyncState>>,
    /// Poison latch (task29): any append/fsync failure latches this and the
    /// store refuses further writes. Authoritative on failure.
    poison: Arc<WalPoison>,
    /// Version-keyed durability watermark (task28). Present in both modes.
    durability: Arc<WalDurability>,
    /// Number of WAL entries sent but not yet fsynced (Eventual mode).
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicU64>,
}

impl WalHandle {
    /// Create a new WAL handle, using the production `FsWrite` sink. Delegates
    /// to [`with_sink_kind`][Self::with_sink_kind]. Both modes use a background
    /// thread for batched writes.
    pub fn new(dir: &Path, consistent: bool, poison: Arc<WalPoison>) -> Result<Self> {
        Self::with_sink_kind(dir, consistent, poison, WalSinkKind::FsWrite)
    }

    /// Build a handle whose sink is chosen at runtime by `kind`. Each match arm
    /// monomorphizes the generic `with_sink` with a concrete sink type.
    pub(crate) fn with_sink_kind(
        dir: &Path,
        consistent: bool,
        poison: Arc<WalPoison>,
        kind: WalSinkKind,
    ) -> Result<Self> {
        match kind {
            WalSinkKind::FsWrite => Ok(Self::with_sink(FileSink::open(dir)?, consistent, poison)),
            WalSinkKind::Coalesced => {
                Ok(Self::with_sink(BufferedFileSink::open(dir, false)?, consistent, poison))
            }
            WalSinkKind::BufferedFile => {
                Ok(Self::with_sink(BufferedFileSink::open(dir, true)?, consistent, poison))
            }
            #[cfg(feature = "bench-internals")]
            WalSinkKind::Mmap => Ok(Self::with_sink(MmapSink::open(dir)?, consistent, poison)),
            #[cfg(all(target_os = "linux", feature = "wal-iouring"))]
            WalSinkKind::IoUring => Ok(Self::with_sink(IoUringSink::open(dir)?, consistent, poison)),
        }
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

        let durability = WalDurability::new();

        let (tx, rx) = mpsc::channel::<WalMsg>();
        let bg_in_flight = in_flight.clone();
        let bg_sync_state = sync_state.clone();
        let bg_poison = poison.clone();
        let bg_durability = durability.clone();

        let handle = spawn_wal_thread(
            sink,
            rx,
            bg_in_flight,
            bg_sync_state,
            bg_poison,
            bg_durability,
        );

        Self {
            sender: Some(tx),
            bg_thread: Some(handle),
            consistent,
            sync_state,
            poison,
            durability,
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
            .send(WalMsg::Entry(entry))
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

    /// Request a prune of entries with version <= `up_to_version`, executed
    /// by the background thread between batches — serialized with appends,
    /// so a concurrent commit's entry can never be caught mid-rewrite and
    /// destroyed. Returns a receiver that yields the prune's result; wait on
    /// it without holding any store lock. A `RecvError` means the WAL
    /// thread stopped (poisoned or shutting down) before pruning.
    pub fn request_prune(&self, up_to_version: u64) -> Result<mpsc::Receiver<Result<()>>> {
        self.poison.check()?;
        let sender = self.sender.as_ref().expect("WalHandle used after drop");
        let (done, rx) = mpsc::channel();
        sender
            .send(WalMsg::Prune {
                up_to_version,
                done,
            })
            .map_err(|e| Error::Persistence(e.to_string()))?;
        Ok(rx)
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

/// Background WAL writer loop. Drains a batch, writes it, fsyncs once, and
/// advances the synced epoch. On any append/fsync failure, poisons the latch,
/// wakes all waiters, and stops.
fn spawn_wal_thread<S: WalSink + 'static>(
    mut sink: S,
    rx: mpsc::Receiver<WalMsg>,
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    sync_state: Option<Arc<WalSyncState>>,
    poison: Arc<WalPoison>,
    durability: Arc<WalDurability>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            // Collect a batch of entries; stop draining at a prune request
            // so it executes after this batch is flushed (and before any
            // later appends — they stay queued in the channel).
            let mut batch = Vec::new();
            let mut prune_req = None;
            match first {
                WalMsg::Entry(e) => batch.push(e),
                WalMsg::Prune {
                    up_to_version,
                    done,
                } => prune_req = Some((up_to_version, done)),
            }
            if prune_req.is_none() {
                while let Ok(msg) = rx.try_recv() {
                    match msg {
                        WalMsg::Entry(e) => batch.push(e),
                        WalMsg::Prune {
                            up_to_version,
                            done,
                        } => {
                            prune_req = Some((up_to_version, done));
                            break;
                        }
                    }
                }
            }

            if batch.is_empty() {
                if let Some((up_to_version, done)) = prune_req {
                    let _ = done.send(sink.prune(up_to_version));
                }
                continue;
            }
            let count = batch.len() as u64;
            // Batch high-water mark: the max version in the batch. Entries
            // arrive in commit order, so this is the last one, but `max` is
            // robust regardless. Used to advance / error the watermark.
            let hwm = batch.iter().map(|e| e.version).max().unwrap_or(0);

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
                    // Batch is durable — release Consistent epoch waiters...
                    if let Some(state) = &sync_state {
                        let _guard = state.mu.lock().unwrap();
                        state
                            .fsynced_epoch
                            .fetch_add(count, std::sync::atomic::Ordering::Release);
                        state.condvar.notify_all();
                    }
                    // ...and advance the version-keyed watermark (task28).
                    durability.publish(hwm);
                    // Batch flushed — now safe to run a pending prune. Any
                    // entry submitted after the prune request is still
                    // queued and will be appended to the rewritten file.
                    if let Some((up_to_version, done)) = prune_req {
                        let _ = done.send(sink.prune(up_to_version));
                    }
                }
                Err(e) => {
                    // Poison is authoritative (task29): latch the store, wake
                    // epoch waiters so they observe it, surface the error to
                    // watermark waiters at/below hwm, and stop the thread. The
                    // epoch/watermark are NOT advanced: this batch never reached
                    // disk. `durability.close()` after the loop releases any
                    // watermark waiter parked above hwm.
                    let msg = format!("WAL durability failure: {e}");
                    poison.poison(msg.clone());
                    if let Some(state) = &sync_state {
                        let _guard = state.mu.lock().unwrap();
                        state.condvar.notify_all();
                    }
                    durability.publish_error(hwm, msg);
                    break;
                }
            }
        }
        // Normal shutdown or post-failure stop: release any watermark waiter
        // parked on a version that will never be reached. Idempotent with the
        // `close()` in `WalHandle::drop`.
        durability.close();
    })
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
// BenchWal — benchmark-only handle (feature `bench-internals`)
// ---------------------------------------------------------------------------

/// Minimal, primitive-typed wrapper around [`WalHandle`] so an external
/// `benches/` crate can drive the WAL directly, in isolation from the rest of
/// the store. Exposes only `WalEntry`/`Path`/`u64`/`Result` in its signatures,
/// so no internal types (`SyncWaiter`, `WalDurability`, …) leak.
///
/// Hidden from docs and gated behind the `bench-internals` feature — it is not
/// part of the stable public API and must not be relied on.
#[doc(hidden)]
#[cfg(feature = "bench-internals")]
pub struct BenchWal {
    inner: WalHandle,
}

#[doc(hidden)]
#[cfg(feature = "bench-internals")]
impl BenchWal {
    /// Open a WAL in `dir`. `consistent` selects Consistent vs Eventual mode;
    /// `kind` selects the sink implementation under test.
    pub fn new(dir: &Path, consistent: bool, kind: WalSinkKind) -> Result<Self> {
        Ok(Self {
            inner: WalHandle::with_sink_kind(dir, consistent, Arc::new(WalPoison::new()), kind)?,
        })
    }

    /// Consistent-mode commit: enqueue the entry and block until the batch it
    /// lands in has been fsynced.
    pub fn commit_consistent(&self, entry: WalEntry) -> Result<()> {
        self.inner.write(entry)?.wait()?;
        Ok(())
    }

    /// Eventual-mode commit: enqueue the entry and return immediately
    /// (fire-and-forget — no fsync wait).
    pub fn commit_eventual(&self, entry: WalEntry) -> Result<()> {
        self.inner.write(entry)?;
        Ok(())
    }

    /// Block until `version` is fsync-durable. Used to drain a fire-and-forget
    /// batch so Eventual throughput includes the real disk cost.
    pub fn wait_durable(&self, version: u64) -> Result<()> {
        self.inner.durability().wait(version)
    }

    /// Highest commit version currently known fsync-durable.
    pub fn durable_version(&self) -> u64 {
        self.inner.durable_version()
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

    #[test]
    fn frame_entry_concatenation_reads_back_via_read_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);
        let e1 = WalEntry { version: 1, ops: vec![WalOp::Insert { table: "t".into(), id: 1, data: vec![1, 2, 3] }] };
        let e2 = WalEntry { version: 2, ops: vec![WalOp::Delete { table: "t".into(), id: 1 }] };

        let mut bytes = frame_entry(&e1).unwrap();
        bytes.extend_from_slice(&frame_entry(&e2).unwrap());
        std::fs::write(&path, &bytes).unwrap();

        let read = read_wal(&path).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].version, 1);
        assert_eq!(read[1].version, 2);
    }

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
                wal_write: crate::WalWrite::PerEntry,
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

    /// Prune requests routed through the WAL background thread are
    /// serialized with appends: entries submitted around the prune — even
    /// immediately after the request, before it executes — must survive,
    /// and the sink must keep appending to the rewritten file (not the old
    /// unlinked inode).
    #[test]
    fn prune_through_wal_thread_keeps_interleaved_appends() {
        for kind in [WalSinkKind::FsWrite, WalSinkKind::Coalesced] {
            let dir = tempfile::tempdir().unwrap();
            let poison = Arc::new(WalPoison::new());
            let handle =
                WalHandle::with_sink_kind(dir.path(), true, Arc::clone(&poison), kind).unwrap();

            for v in 1..=3u64 {
                handle
                    .write(WalEntry {
                        version: v,
                        ops: vec![],
                    })
                    .unwrap()
                    .wait()
                    .unwrap();
            }

            let rx = handle.request_prune(2).unwrap();
            // Submitted right behind the prune request — must land in the
            // rewritten file.
            for v in 4..=5u64 {
                handle
                    .write(WalEntry {
                        version: v,
                        ops: vec![],
                    })
                    .unwrap();
            }
            rx.recv().unwrap().unwrap();
            drop(handle); // joins the bg thread, flushing everything

            let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
            let versions: Vec<u64> = entries.iter().map(|e| e.version).collect();
            assert_eq!(
                versions,
                vec![3, 4, 5],
                "sink kind {kind:?}: pruned WAL lost or kept wrong entries"
            );
        }
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

    // --- task28: version-keyed durability watermark ---------------------------

    /// In Eventual mode there is no `SyncWaiter`, but the watermark still lets a
    /// caller observe when a committed version became fsync-durable.
    #[test]
    fn durability_watermark_advances_in_eventual_mode() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
        let handle = WalHandle::new(dir.path(), true, Arc::new(WalPoison::new())).unwrap();
        let w = handle.write(WalEntry { version: 7, ops: vec![] }).unwrap();
        w.wait().unwrap();
        handle.durability().wait(7).unwrap();
        assert!(handle.durable_version() >= 7);
    }

    /// Waiting on an already-durable version returns immediately (no block).
    #[test]
    fn wait_durable_on_already_durable_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
        let handle = WalHandle::new(dir.path(), false, Arc::new(WalPoison::new())).unwrap();
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
                wal_write: crate::WalWrite::PerEntry,
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

    #[test]
    fn with_sink_kind_coalesced_writes_recoverable_wal() {
        let dir = tempfile::tempdir().unwrap();
        let poison = Arc::new(WalPoison::new());
        {
            let h = WalHandle::with_sink_kind(dir.path(), true, poison, WalSinkKind::Coalesced).unwrap();
            h.write(WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] })
                .unwrap()
                .wait()
                .unwrap();
        }
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].version, 1);
    }

    #[test]
    fn with_sink_kind_fswrite_writes_recoverable_wal() {
        let dir = tempfile::tempdir().unwrap();
        let poison = Arc::new(WalPoison::new());
        {
            let h = WalHandle::with_sink_kind(dir.path(), true, poison, WalSinkKind::FsWrite).unwrap();
            h.write(WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] })
                .unwrap()
                .wait()
                .unwrap();
        } // drop joins bg thread, fsyncs
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].version, 1);
    }

    #[test]
    fn buffered_file_sink_roundtrips_via_read_wal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sink = BufferedFileSink::open(dir.path(), true).unwrap();
            for v in 1..=5u64 {
                sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
            }
            sink.sync().unwrap();
        }
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len(), 5);
        assert_eq!(read[4].version, 5);
    }

    #[test]
    fn buffered_file_sink_sync_all_roundtrips_via_read_wal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sink = BufferedFileSink::open(dir.path(), false).unwrap(); // sync_all
            for v in 1..=5u64 {
                sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
            }
            sink.sync().unwrap();
        }
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len(), 5);
        assert_eq!(read[4].version, 5);
    }

    #[test]
    fn read_wal_stops_at_zero_length_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(WAL_FILENAME);
        // One valid record followed by a zero tail (as a pre-sized mmap crash leaves).
        let mut bytes = frame_entry(&WalEntry { version: 7, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
        bytes.extend_from_slice(&[0u8; 64]);
        std::fs::write(&path, &bytes).unwrap();

        let read = read_wal(&path).unwrap(); // must NOT return Err(WalCorrupted)
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].version, 7);
    }

    #[cfg(feature = "bench-internals")]
    #[test]
    fn mmap_sink_roundtrips_via_read_wal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sink = MmapSink::open(dir.path()).unwrap();
            for v in 1..=5u64 {
                sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![v as u8; 32] }] }).unwrap();
            }
            sink.sync().unwrap();
        } // Drop truncates to logical length + syncs
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len(), 5);
        assert_eq!(read[4].version, 5);
    }

    #[cfg(feature = "bench-internals")]
    #[test]
    fn mmap_sink_recovers_after_growing_past_quantum() {
        let dir = tempfile::tempdir().unwrap();
        // ~9.6 MiB of records forces at least one grow past the 8 MiB quantum.
        let n = 600u64;
        {
            let mut sink = MmapSink::open(dir.path()).unwrap();
            for v in 1..=n {
                sink.append(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), id: v, data: vec![0u8; 16 * 1024] }] }).unwrap();
            }
            sink.sync().unwrap();
        }
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len() as u64, n);
        assert_eq!(read[(n - 1) as usize].version, n);
    }

    #[cfg(all(target_os = "linux", feature = "wal-iouring"))]
    #[test]
    fn iouring_sink_roundtrips_via_read_wal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut sink = IoUringSink::open(dir.path()).unwrap();
            for v in 1..=5u64 {
                sink.append(&WalEntry {
                    version: v,
                    ops: vec![WalOp::Insert {
                        table: "t".into(),
                        id: v,
                        data: vec![v as u8; 32],
                    }],
                })
                .unwrap();
            }
            sink.sync().unwrap();
        }
        let read = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(read.len(), 5);
        assert_eq!(read[4].version, 5);
    }

    /// Eventual mode has no waiter, but an fsync failure must still poison the
    /// shared latch so the store's next begin_write/commit is refused.
    #[test]
    fn wal_eventual_sync_failure_poisons() {
        let poison = Arc::new(WalPoison::new());
        let handle = WalHandle::with_sink(
            FaultySink {
                fail_sync_after: 0, // fail immediately
                sync_count: 0,
            },
            false, // Eventual: write() returns SyncWaiter::Done
            poison.clone(),
        );
        handle
            .write(WalEntry { version: 1, ops: vec![] })
            .unwrap();
        // Dropping the handle joins the background thread, which by then has
        // processed the entry, failed the fsync, and poisoned the latch.
        drop(handle);
        assert!(
            poison.is_poisoned(),
            "Eventual-mode fsync failure must poison the latch"
        );
    }
}
