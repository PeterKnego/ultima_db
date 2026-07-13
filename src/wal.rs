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
use std::sync::mpsc;
use std::thread;

use parking_lot::{Condvar, Mutex};

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
        let mut c = self.cause.lock();
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
        let c = self.cause.lock();
        Error::Poisoned(c.clone().unwrap_or_else(|| "WAL poisoned".into()))
    }
}

// ---------------------------------------------------------------------------
// WAL data types
// ---------------------------------------------------------------------------

/// A single mutation within a transaction.
#[derive(Debug, Clone)]
pub enum WalOp {
    /// A new row was inserted.
    Insert {
        /// Name of the table the row was inserted into.
        table: String,
        /// Primary key of the inserted row.
        id: u64,
        /// Bincode-serialized record bytes.
        data: Vec<u8>,
    },
    /// An existing row was overwritten.
    Update {
        /// Name of the table the row belongs to.
        table: String,
        /// Primary key of the updated row.
        id: u64,
        /// Bincode-serialized record bytes (the new value).
        data: Vec<u8>,
    },
    /// A row was removed.
    Delete {
        /// Name of the table the row was removed from.
        table: String,
        /// Primary key of the deleted row.
        id: u64,
    },
    /// A new (empty) table was created.
    CreateTable {
        /// Name of the created table.
        name: String,
    },
    /// A table was dropped.
    DeleteTable {
        /// Name of the deleted table.
        name: String,
    },
    /// Marker recording that a bulk load replaced `tables` at this entry's
    /// version. Bulk-loaded data itself is not WAL-logged; the marker lets
    /// recovery detect WAL commits that were made on top of a load no
    /// checkpoint covers (such commits cannot be replayed against pre-load
    /// state) and fail with [`Error::BulkLoadNotCheckpointed`] instead of
    /// silently producing a state no client ever observed.
    BulkLoad {
        /// Names of the tables the bulk load replaced.
        tables: Vec<String>,
    },
}

/// A complete WAL entry for one committed transaction.
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Commit version this entry belongs to; matches the `Snapshot` version
    /// produced by the corresponding `WriteTx::commit`.
    pub version: u64,
    /// The ordered row-level mutations that made up the transaction.
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

    // Cap the preallocation: `op_count` comes from the file, and the CRC only
    // guards against accidental corruption — a crafted count must not drive a
    // huge allocation. The Vec grows normally past the hint if needed.
    let mut ops = Vec::with_capacity(op_count.min(1024) as usize);
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
    // Standard CRC-32/ISO-HDLC (IEEE 802.3), hardware-accelerated via `crc32fast`
    // (already used by `snapshot_stream`). Byte-identical to the previous
    // hand-rolled bitwise loop — guarded by `crc32_equivalent_to_reference_bitwise_and_standard`
    // — so existing WAL and checkpoint files keep verifying.
    crc32fast::hash(data)
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

/// Physically zero-fill `[from, to)` with real writes (NOT sparse `set_len`,
/// so ext4 marks the extents *written*), then `sync_all` once so the size and
/// allocation are durable before any record is written into the region. The
/// WAL counterpart of `ultima_journal`'s `SegmentFile::preallocate_to`. No-op
/// when `to <= from`.
fn preallocate_to(file: &mut File, from: u64, to: u64) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    if to <= from {
        return Ok(());
    }
    let zeros = [0u8; 64 * 1024];
    file.seek(SeekFrom::Start(from)).map_err(|e| Error::Persistence(e.to_string()))?;
    let mut remaining = to - from;
    while remaining > 0 {
        let n = remaining.min(zeros.len() as u64) as usize;
        file.write_all(&zeros[..n]).map_err(|e| Error::Persistence(e.to_string()))?;
        remaining -= n as u64;
    }
    file.sync_all().map_err(|e| Error::Persistence(e.to_string()))?;
    Ok(())
}

/// Scan framed WAL records. Returns the decoded entries and the byte offset
/// where scanning stopped (end of the last good record = the durable write
/// head). A zero len-prefix and a truncated tail are always end-of-log. When
/// `tail_tolerant`, a CRC mismatch or undecodable frame is *also* treated as
/// end-of-log (a torn write into preallocated zero space looks complete); when
/// not, a CRC mismatch is a hard `WalCorrupted` error (strict corruption
/// detection for the non-preallocated path).
pub(crate) fn scan_wal(path: &Path, tail_tolerant: bool) -> Result<(Vec<WalEntry>, u64)> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(e) => return Err(Error::Persistence(e.to_string())),
    };
    let mut all_bytes = Vec::new();
    file.read_to_end(&mut all_bytes)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    let mut entries = Vec::new();
    let mut offset = 0usize;

    while offset + 4 <= all_bytes.len() {
        let len = u32::from_le_bytes(all_bytes[offset..offset + 4].try_into().unwrap()) as usize;
        if len == 0 {
            break; // zero len-prefix: clean end-of-log / preallocated tail
        }
        if offset + 4 + len + 4 > all_bytes.len() {
            break; // truncated tail (crash during write)
        }
        let data = &all_bytes[offset + 4..offset + 4 + len];
        let stored_crc =
            u32::from_le_bytes(all_bytes[offset + 4 + len..offset + 8 + len].try_into().unwrap());
        if crc32(data) != stored_crc {
            if tail_tolerant {
                break; // torn write into preallocated space: stop at last good record
            }
            return Err(Error::WalCorrupted(format!(
                "CRC mismatch at entry starting at byte {offset}"
            )));
        }
        match deserialize_entry(data) {
            Ok(entry) => entries.push(entry),
            Err(e) if tail_tolerant => {
                let _ = e;
                break;
            }
            Err(e) => return Err(e),
        }
        offset += 4 + len + 4;
    }

    Ok((entries, offset as u64))
}

/// Read all WAL entries from a file. Strict: stops at EOF / zero tail, errors
/// on CRC mismatch. Unchanged behavior for all existing callers.
pub fn read_wal(path: &Path) -> Result<Vec<WalEntry>> {
    Ok(scan_wal(path, false)?.0)
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

/// Preallocating prune (design §6, strategy P2): rewrite the live entries
/// (version > `up_to_version`) into a tmp that is first zero-filled to
/// `live_bytes + chunk`, then atomically rename it over `path`. The renamed
/// file is already preallocated. Returns `Some((write_head, capacity))` after a
/// rewrite, or `None` if nothing needed pruning. Crash-atomic via tmp+rename:
/// a crash leaves either the complete old WAL or the complete new one.
/// Prune a preallocated WAL file, using a tolerant scan consistent with the
/// prealloc recovery model: a torn record in the zero tail is end-of-log, not
/// corruption. This mirrors `PreallocFileSink::open` which also uses
/// `scan_wal(path, true)`. Entries past the first bad CRC were never durable
/// (they are in the zero tail), so stopping there is correct.
fn prune_wal_prealloc(path: &Path, up_to_version: u64, chunk: u64) -> Result<Option<(u64, u64)>> {
    use std::io::{Seek, SeekFrom, Write};
    let (entries, _) = scan_wal(path, true)?;
    let remaining: Vec<&WalEntry> = entries.iter().filter(|e| e.version > up_to_version).collect();
    if remaining.len() == entries.len() {
        return Ok(None); // nothing to prune
    }

    let mut live = Vec::new();
    for e in &remaining {
        live.extend_from_slice(&frame_entry(e)?);
    }
    let write_head = live.len() as u64;
    let capacity = (write_head + chunk).div_ceil(chunk) * chunk;

    let tmp_path = path.with_file_name(format!(
        "{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));
    let mut tmp = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&tmp_path)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    // Pre-size to capacity with real zeros (single sync_all), then overwrite the
    // front with the live entries.
    preallocate_to(&mut tmp, 0, capacity)?;
    tmp.seek(SeekFrom::Start(0)).map_err(|e| Error::Persistence(e.to_string()))?;
    tmp.write_all(&live).map_err(|e| Error::Persistence(e.to_string()))?;
    tmp.sync_all().map_err(|e| Error::Persistence(e.to_string()))?;
    drop(tmp);
    std::fs::rename(&tmp_path, path).map_err(|e| Error::Persistence(e.to_string()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(Some((write_head, capacity)))
}

// ---------------------------------------------------------------------------
// Epoch-based sync state — shared between WalHandle and SyncWaiter
// ---------------------------------------------------------------------------

/// Tracks which WAL epoch has been fsynced. Writers obtain an epoch via
/// `next_epoch`, then wait until `fsynced_epoch >= their_epoch`.
pub(crate) struct WalSyncState {
    pub(crate) next_epoch: std::sync::atomic::AtomicU64,
    fsynced_epoch: std::sync::atomic::AtomicU64,
    condvar: Condvar,
    mu: Mutex<()>,
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
    /// Inline-fsync (no bg thread): the committing thread performs the
    /// append+fsync in `wait()`, off the store lock. Durable on `Ok`.
    InlineSync {
        sink: Arc<Mutex<Box<dyn WalSink>>>,
        entry: WalEntry,
        durability: Arc<WalDurability>,
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
        match self {
            SyncWaiter::Done => Ok(()),
            SyncWaiter::WaitForEpoch { epoch, state, poison } => {
                let mut guard = state.mu.lock();
                loop {
                    if state.fsynced_epoch.load(std::sync::atomic::Ordering::Acquire) >= epoch {
                        return Ok(());
                    }
                    if poison.is_poisoned() {
                        return Err(poison.error());
                    }
                    state.condvar.wait(&mut guard);
                }
            }
            SyncWaiter::InlineSync { sink, entry, durability, poison } => {
                poison.check()?;
                let version = entry.version;
                let mut s = sink.lock();
                let res: Result<()> = (|| {
                    s.append(&entry)?;
                    s.sync()
                })();
                match res {
                    Ok(()) => {
                        durability.publish(version);
                        Ok(())
                    }
                    Err(e) => {
                        let msg = format!("WAL durability failure: {e}");
                        poison.poison(msg.clone());
                        durability.publish_error(version, msg);
                        Err(e)
                    }
                }
            }
        }
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
/// default), `Coalesced`, and `CoalescedPrealloc` are production-safe; the
/// remaining variants (`BufferedFile`, `Mmap`, `IoUring`) are experimental and
/// bench-only.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalSinkKind {
    /// Baseline: one `write_all` per entry (the framed record) + `sync_all` per batch.
    FsWrite,
    /// Coalesced single `write` per batch + `sync_all` (fsync). Production-safe.
    Coalesced,
    /// Coalesced single `write` per batch + `sync_data` (fdatasync). Bench comparison only.
    BufferedFile,
    /// Preallocating coalesced sink (`PreallocFileSink`). Production opt-in.
    CoalescedPrealloc,
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
// PreallocFileSink — production preallocating WAL sink
// ---------------------------------------------------------------------------

/// Default grow quantum for the preallocating WAL sink.
const WAL_PREALLOC_CHUNK: u64 = 16 * 1024 * 1024;

/// Production preallocating sink: positioned writes into a physically
/// zero-filled region of `wal.bin`, grown inline in `chunk`-byte steps.
/// `sync_all` only on extend (size change); `sync_data` steady-state. See
/// design doc 2026-06-20-wal-preallocation-design.md.
struct PreallocFileSink {
    file: File,
    path: std::path::PathBuf,
    buf: Vec<u8>,
    write_head: u64,
    capacity: u64,
    chunk: u64,
}

impl PreallocFileSink {
    fn open(dir: &Path) -> Result<Self> {
        Self::open_with_chunk(dir, WAL_PREALLOC_CHUNK)
    }

    pub(crate) fn open_with_chunk(dir: &Path, chunk: u64) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;
        let path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        sync_dir(dir)?;
        // Reconstruct the write head from the tolerant scan; a torn tail into
        // preallocated zeros is end-of-log, not corruption.
        let (_entries, write_head) = scan_wal(&path, true)?;
        let capacity = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        Ok(PreallocFileSink { file, path, buf: Vec::new(), write_head, capacity, chunk })
    }
}

impl WalSink for PreallocFileSink {
    fn append(&mut self, entry: &WalEntry) -> Result<()> {
        self.buf.extend_from_slice(&frame_entry(entry)?);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        if !self.buf.is_empty() {
            let need = self.write_head + self.buf.len() as u64;
            if need > self.capacity {
                // Extend by whole chunks to cover `need`; sync_all (size change).
                let new_cap = need.div_ceil(self.chunk) * self.chunk;
                preallocate_to(&mut self.file, self.capacity, new_cap)?;
                self.capacity = new_cap;
            }
            self.file.seek(SeekFrom::Start(self.write_head)).map_err(|e| Error::Persistence(e.to_string()))?;
            self.file.write_all(&self.buf).map_err(|e| Error::Persistence(e.to_string()))?;
            self.write_head += self.buf.len() as u64;
            self.buf.clear();
        }
        // Steady-state barrier: size unchanged, so fdatasync suffices.
        self.file.sync_data().map_err(|e| Error::Persistence(e.to_string()))
    }

    fn prune(&mut self, up_to_version: u64) -> Result<()> {
        if let Some((write_head, capacity)) = prune_wal_prealloc(&self.path, up_to_version, self.chunk)? {
            // Reopen the renamed (new) inode and adopt the recomputed cursors.
            // create(false): the rename guarantees the file exists; ENOENT here
            // means the rename failed, and we want a loud error, not a silent
            // empty-WAL creation.
            self.file = OpenOptions::new()
                .read(true).write(true).create(false).truncate(false)
                .open(&self.path)
                .map_err(|e| Error::Persistence(e.to_string()))?;
            self.write_head = write_head;
            self.capacity = capacity;
            self.buf.clear();
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
    inner: Mutex<DurabilityWaiters>,
    condvar: Condvar,
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
            inner: Mutex::new(DurabilityWaiters::default()),
            condvar: Condvar::new(),
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
            let mut w = self.inner.lock();
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
            let mut w = self.inner.lock();
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
            let mut w = self.inner.lock();
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
        let mut guard = self.inner.lock();
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
            self.condvar.wait(&mut guard);
        }
    }

    /// Register `cb` to fire once `version` is durable. Fires inline (on the
    /// calling thread) if already durable, already errored, or already closed.
    pub(crate) fn on_complete(&self, version: u64, cb: DurabilityCallback) {
        use std::sync::atomic::Ordering;
        let mut w = self.inner.lock();
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
    /// When `Some`, this handle has NO background thread —
    /// `write()` stages an `InlineSync` waiter (no I/O yet); the caller drives
    /// the actual append+fsync by calling `wait()` off the store lock. This
    /// eliminates the enqueue→wake-writer→fsync→wake-waiter handoff
    /// (~20–35µs/commit) that only pays off when commits can batch. Wired for
    /// `SingleWriter + Consistent` (serial commits never batch). See
    /// `docs/tasks/task38_wal_inline_fsync.md`.
    sync_sink: Option<Arc<Mutex<Box<dyn WalSink>>>>,
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
            WalSinkKind::CoalescedPrealloc => {
                Ok(Self::with_sink(PreallocFileSink::open(dir)?, consistent, poison))
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
                condvar: Condvar::new(),
                mu: Mutex::new(()),
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
            sync_sink: None,
        }
    }

    /// Build a handle with NO background thread. `write()`
    /// appends + fsyncs on the caller's thread (see `sync_sink`). `consistent`
    /// is accepted for symmetry but the path is always synchronous-durable.
    pub(crate) fn with_sink_inline<S: WalSink + 'static>(
        sink: S,
        consistent: bool,
        poison: Arc<WalPoison>,
    ) -> Self {
        Self {
            sender: None,
            bg_thread: None,
            consistent,
            sync_state: None,
            poison,
            durability: WalDurability::new(),
            in_flight: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sync_sink: Some(Arc::new(Mutex::new(Box::new(sink)))),
        }
    }

    /// Inline counterpart of [`with_sink_kind`].
    pub(crate) fn with_sink_kind_inline(
        dir: &Path,
        consistent: bool,
        poison: Arc<WalPoison>,
        kind: WalSinkKind,
    ) -> Result<Self> {
        Ok(match kind {
            WalSinkKind::FsWrite => Self::with_sink_inline(FileSink::open(dir)?, consistent, poison),
            WalSinkKind::Coalesced => {
                Self::with_sink_inline(BufferedFileSink::open(dir, false)?, consistent, poison)
            }
            WalSinkKind::BufferedFile => {
                Self::with_sink_inline(BufferedFileSink::open(dir, true)?, consistent, poison)
            }
            WalSinkKind::CoalescedPrealloc => {
                Self::with_sink_inline(PreallocFileSink::open(dir)?, consistent, poison)
            }
            #[cfg(feature = "bench-internals")]
            WalSinkKind::Mmap => Self::with_sink_inline(MmapSink::open(dir)?, consistent, poison),
            #[cfg(all(target_os = "linux", feature = "wal-iouring"))]
            WalSinkKind::IoUring => Self::with_sink_inline(IoUringSink::open(dir)?, consistent, poison),
        })
    }

    /// Submit a WAL entry to the background thread.
    ///
    /// - **Consistent**: returns `SyncWaiter::WaitForEpoch` — caller must
    ///   call `wait()` outside the store lock to block until fsync.
    /// - **Eventual**: returns `SyncWaiter::Done` — no wait needed.
    pub fn write(&self, entry: WalEntry) -> Result<SyncWaiter> {
        // Inline (no bg thread): do NO I/O here (this runs under store_inner).
        // Return a waiter; the committer does append+fsync off-lock in wait().
        if let Some(sink) = &self.sync_sink {
            self.poison.check()?;
            return Ok(SyncWaiter::InlineSync {
                sink: Arc::clone(sink),
                entry,
                durability: Arc::clone(&self.durability),
                poison: Arc::clone(&self.poison),
            });
        }
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
        // Prune on this thread; return a ready receiver so
        // the API shape (caller waits on the channel) is unchanged.
        if let Some(sink) = &self.sync_sink {
            let (done, rx) = mpsc::channel();
            let res = sink.lock().prune(up_to_version);
            let _ = done.send(res);
            return Ok(rx);
        }
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
                        let _guard = state.mu.lock();
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
                        let _guard = state.mu.lock();
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
    pub(crate) entries: Mutex<Vec<WalEntry>>,
    sync_state: Arc<WalSyncState>,
    poison: Arc<WalPoison>,
}

#[cfg(test)]
impl MockWal {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            sync_state: Arc::new(WalSyncState {
                next_epoch: std::sync::atomic::AtomicU64::new(1),
                fsynced_epoch: std::sync::atomic::AtomicU64::new(0),
                condvar: Condvar::new(),
                mu: Mutex::new(()),
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
        let _guard = self.sync_state.mu.lock();
        self.sync_state.condvar.notify_all();
    }

    /// Submit a WAL entry. Returns a SyncWaiter that blocks until `flush()` or
    /// `fail()`.
    pub fn write(&self, entry: WalEntry) -> SyncWaiter {
        self.entries.lock().push(entry);
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
        let _guard = self.sync_state.mu.lock();
        self.sync_state.condvar.notify_all();
    }

    /// Advance fsynced_epoch by 1, releasing only the next blocked waiter.
    pub fn flush_one(&self) {
        self.sync_state
            .fsynced_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        let _guard = self.sync_state.mu.lock();
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
    fn scan_wal_returns_durable_offset_and_strict_wrapper_matches() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);
        // Two framed entries written back-to-back.
        let e1 = WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] };
        let e2 = WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] };
        let mut bytes = frame_entry(&e1).unwrap();
        let f1_len = bytes.len() as u64;
        bytes.extend_from_slice(&frame_entry(&e2).unwrap());
        let total = bytes.len() as u64;
        std::fs::write(&path, &bytes).unwrap();

        let (entries, offset) = scan_wal(&path, false).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(offset, total, "offset is end of last good record");
        assert!(f1_len > 0);
        // Strict wrapper returns the same entries.
        assert_eq!(read_wal(&path).unwrap().len(), 2);
    }

    #[test]
    fn scan_wal_tolerant_stops_at_crc_mismatch_strict_errors() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);
        let good = WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] };
        let mut bytes = frame_entry(&good).unwrap();
        let good_len = bytes.len() as u64;
        // A second frame with a corrupted CRC, then a zero tail (preallocated space).
        let mut torn = frame_entry(&WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] }).unwrap();
        let last = torn.len() - 1;
        torn[last] ^= 0xFF; // flip a CRC byte
        bytes.extend_from_slice(&torn);
        bytes.extend_from_slice(&[0u8; 4096]); // durable zeros after the torn record
        std::fs::write(&path, &bytes).unwrap();

        // Tolerant: stop at the good record, no error, offset = end of good record.
        let (entries, offset) = scan_wal(&path, true).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(offset, good_len);
        // Strict: still flags corruption.
        assert!(matches!(scan_wal(&path, false), Err(Error::WalCorrupted(_))));
    }

    #[test]
    fn frame_entry_concatenation_reads_back_via_read_wal() {
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
    fn inline_write_is_durable_and_recoverable() {
        // Off-lock inline: write() returns InlineSync (no I/O yet); wait() does
        // the append+fsync and makes it durable.
        let dir = crate::test_scratch::scratch_dir();
        let poison = Arc::new(WalPoison::new());
        {
            let wal = WalHandle::with_sink_inline(
                FileSink::open(dir.path()).unwrap(),
                true,
                poison,
            );
            for v in 1..=5u64 {
                let w = wal
                    .write(WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] })
                    .unwrap();
                assert!(matches!(w, SyncWaiter::InlineSync { .. }), "inline write returns InlineSync");
                w.wait().unwrap(); // performs the append+fsync off-lock
            }
            assert_eq!(wal.durable_version(), 5);
        }
        let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(entries.iter().map(|e| e.version).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn crc32_equivalent_to_reference_bitwise_and_standard() {
        // Reference: the textbook reflected CRC-32/ISO-HDLC (IEEE 802.3) — the
        // exact algorithm the WAL/checkpoint format was written with. The crate's
        // `crc32()` must stay byte-identical to this for every existing WAL and
        // checkpoint file to keep verifying, regardless of the implementation.
        fn reference(data: &[u8]) -> u32 {
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
        // Standard CRC-32 check value: crc32("123456789") == 0xCBF4_3926. Anchors
        // both `crc32()` and the reference to the IEEE standard absolutely.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(reference(b"123456789"), 0xCBF4_3926);
        // Equivalence across a spread of inputs: empty, every single byte value,
        // ascending sequences, text, and a large buffer.
        let mut cases: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0u8],
            vec![0xFFu8],
            b"hello world".to_vec(),
            (0u8..=255).collect(),
            (0u8..=255).cycle().take(4096).collect(),
        ];
        for b in 0u16..256 {
            cases.push(vec![b as u8]);
        }
        for c in &cases {
            assert_eq!(crc32(c), reference(c), "crc32 mismatch for {}-byte input", c.len());
        }
    }

    #[test]
    fn wal_crc_corruption_detected() {
        let dir = crate::test_scratch::scratch_dir();
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
            let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
        let handle = WalHandle::new(dir.path(), true, Arc::new(WalPoison::new())).unwrap();
        let w = handle.write(WalEntry { version: 7, ops: vec![] }).unwrap();
        w.wait().unwrap();
        handle.durability().wait(7).unwrap();
        assert!(handle.durable_version() >= 7);
    }

    /// Waiting on an already-durable version returns immediately (no block).
    #[test]
    fn wait_durable_on_already_durable_is_immediate() {
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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

    /// A crafted WAL entry with a valid CRC but an absurd op count must
    /// produce `WalCorrupted`, not attempt a multi-gigabyte preallocation.
    /// (The CRC only guards against accidental corruption.)
    #[test]
    fn wal_huge_op_count_errors_not_allocates() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);

        let config = bincode::config::standard();
        let mut data = Vec::new();
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // version
        bincode::encode_into_std_write(u32::MAX, &mut data, config).unwrap(); // op_count
        // No op bytes follow — the count is a lie.

        let mut file = File::create(&path).unwrap();
        let len = data.len() as u32;
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&data).unwrap();
        let crc = crc32(&data);
        file.write_all(&crc.to_le_bytes()).unwrap();
        file.sync_all().unwrap();

        let res = read_wal(&path);
        assert!(
            matches!(res, Err(Error::WalCorrupted(_))),
            "expected WalCorrupted, got {res:?}"
        );
    }

    /// Unknown op tag in WAL entry data triggers WalCorrupted error.
    #[test]
    fn wal_unknown_op_tag() {
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
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

    #[test]
    fn prealloc_sink_roundtrips_like_buffered() {
        let dir = crate::test_scratch::scratch_dir();
        let mut sink = PreallocFileSink::open(dir.path()).unwrap();
        sink.append(&WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
        sink.append(&WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] }).unwrap();
        sink.sync().unwrap();
        let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].version, 1);
        assert_eq!(entries[1].version, 2);
    }

    #[test]
    fn prealloc_sink_extends_in_chunks_and_holds_invariant() {
        let dir = crate::test_scratch::scratch_dir();
        // Tiny 256-byte chunk so a couple of entries force an extend.
        let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 256).unwrap();
        let physical = |p: &std::path::Path| std::fs::metadata(p).unwrap().len();
        let path = dir.path().join(WAL_FILENAME);

        for v in 1..=20u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("table-{v}") }] }).unwrap();
            sink.sync().unwrap();
            // Invariant: write_head <= capacity <= physical_len, capacity chunk-aligned.
            assert!(sink.write_head <= sink.capacity);
            assert!(sink.capacity <= physical(&path));
            assert_eq!(sink.capacity % 256, 0, "capacity grows in whole chunks");
        }
        assert_eq!(read_wal(&path).unwrap().len(), 20);
    }

    #[test]
    fn prealloc_sink_steady_state_does_not_grow_physical() {
        let dir = crate::test_scratch::scratch_dir();
        let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 1 << 20).unwrap();
        let path = dir.path().join(WAL_FILENAME);
        sink.append(&WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
        sink.sync().unwrap();
        let after_first = std::fs::metadata(&path).unwrap().len();
        // A second small batch fits in the existing chunk: no physical growth.
        sink.append(&WalEntry { version: 2, ops: vec![WalOp::CreateTable { name: "u".into() }] }).unwrap();
        sink.sync().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), after_first);
    }

    #[test]
    fn prune_wal_prealloc_compacts_and_presizes() {
        let dir = crate::test_scratch::scratch_dir();
        let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
        for v in 1..=5u64 {
            sink.append(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] }).unwrap();
            sink.sync().unwrap();
        }
        // Prune everything <= version 3.
        sink.prune(3).unwrap();
        let path = dir.path().join(WAL_FILENAME);
        let entries = read_wal(&path).unwrap();
        assert_eq!(entries.iter().map(|e| e.version).collect::<Vec<_>>(), vec![4, 5]);
        // File is preallocated: physical_len == write_head rounded up + at least one chunk of zeros.
        let physical = std::fs::metadata(&path).unwrap().len();
        assert!(sink.capacity <= physical);
        assert!(sink.capacity >= sink.write_head + 4096, "a fresh chunk of zero tail exists");
        assert_eq!(sink.capacity % 4096, 0);
    }

    #[test]
    fn prune_then_append_recovers() {
        let dir = crate::test_scratch::scratch_dir();
        {
            let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
            for v in 1..=4u64 {
                sink.append(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] }).unwrap();
                sink.sync().unwrap();
            }
            sink.prune(2).unwrap();
            sink.append(&WalEntry { version: 5, ops: vec![WalOp::CreateTable { name: "t5".into() }] }).unwrap();
            sink.sync().unwrap();
        } // drop = simulated crash (no clean truncation)
        // Reopen and confirm the post-prune appends survive with no gap.
        let sink2 = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
        let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(entries.iter().map(|e| e.version).collect::<Vec<_>>(), vec![3, 4, 5]);
        assert_eq!(sink2.write_head, entries.iter().map(|e| frame_entry(e).unwrap().len() as u64).sum::<u64>());
    }

    #[test]
    fn prune_wal_prealloc_noop_when_nothing_to_prune() {
        let dir = crate::test_scratch::scratch_dir();
        let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
        sink.append(&WalEntry { version: 9, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap();
        sink.sync().unwrap();
        // up_to_version below the only entry's version: nothing removed.
        assert!(prune_wal_prealloc(&dir.path().join(WAL_FILENAME), 1, 4096).unwrap().is_none());
    }

    #[test]
    fn preallocate_to_zero_fills_and_is_durable() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join("p.bin");
        let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        preallocate_to(&mut f, 0, 8192).unwrap();
        assert_eq!(f.metadata().unwrap().len(), 8192, "physically extended");
        // All zeros.
        let mut buf = Vec::new();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.read_to_end(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        // A positioned overwrite within the region does not change the size.
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[7u8; 16]).unwrap();
        assert_eq!(f.metadata().unwrap().len(), 8192);
        // No-op when to <= from.
        preallocate_to(&mut f, 8192, 4096).unwrap();
        assert_eq!(f.metadata().unwrap().len(), 8192);
    }
}
