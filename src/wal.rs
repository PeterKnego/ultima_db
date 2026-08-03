// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Write-ahead log (WAL) for durable persistence.
//!
//! The WAL records row-level deltas for each committed transaction. In
//! `Standalone` persistence mode, WAL entries are written to disk on commit.
//!
//! File format: append-only sequence of `[len: u32][WalEntry bytes][crc32: u32]`.
//! Each `WalEntry` payload opens with `[magic: u8 = 0xFF][format: u8 = 2]` — see
//! [`WAL_ENTRY_MAGIC`] for why that pair cannot be confused with a pre-0.3.0
//! (v1) payload, which had no marker at all.

#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;

use parking_lot::{Condvar, Mutex};

use crate::primary_key::check_encoded_key_len;
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
        /// [`PrimaryKey::KEY_TYPE_ID`](crate::PrimaryKey::KEY_TYPE_ID) of the
        /// key type `key` was encoded with. See [`WalOp::key_type`].
        key_type: u32,
        /// Encoded primary key of the inserted row
        /// ([`PrimaryKey::encode`](crate::PrimaryKey::encode)).
        key: Vec<u8>,
        /// Bincode-serialized record bytes.
        data: Vec<u8>,
    },
    /// An existing row was overwritten.
    Update {
        /// Name of the table the row belongs to.
        table: String,
        /// [`PrimaryKey::KEY_TYPE_ID`](crate::PrimaryKey::KEY_TYPE_ID) of the
        /// key type `key` was encoded with. See [`WalOp::key_type`].
        key_type: u32,
        /// Encoded primary key of the updated row
        /// ([`PrimaryKey::encode`](crate::PrimaryKey::encode)).
        key: Vec<u8>,
        /// Bincode-serialized record bytes (the new value).
        data: Vec<u8>,
    },
    /// A row was removed.
    Delete {
        /// Name of the table the row was removed from.
        table: String,
        /// [`PrimaryKey::KEY_TYPE_ID`](crate::PrimaryKey::KEY_TYPE_ID) of the
        /// key type `key` was encoded with. See [`WalOp::key_type`].
        key_type: u32,
        /// Encoded primary key of the deleted row
        /// ([`PrimaryKey::encode`](crate::PrimaryKey::encode)).
        key: Vec<u8>,
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

impl WalOp {
    /// The key type this op's encoded key was written with
    /// ([`PrimaryKey::KEY_TYPE_ID`](crate::PrimaryKey::KEY_TYPE_ID)), or
    /// `None` for the ops that carry no key.
    ///
    /// Recorded per op, not per entry: one transaction can write to tables
    /// with different key types. Recovery compares it against the key type
    /// the table is registered with in *this* build and refuses to replay on
    /// a mismatch, because an encoded key is opaque bytes that a different
    /// key type will decode without complaint — the eight bytes of `1u64`
    /// are a valid NUL-filled `String`, and an `i64` reading of a `u64` key
    /// differs only in the sign bit. The encoding is order-preserving, so the
    /// reinterpreted keys go on to pass every downstream check; without this
    /// tag nothing in the pipeline notices.
    #[must_use]
    pub fn key_type(&self) -> Option<u32> {
        match self {
            WalOp::Insert { key_type, .. }
            | WalOp::Update { key_type, .. }
            | WalOp::Delete { key_type, .. } => Some(*key_type),
            WalOp::CreateTable { .. } | WalOp::DeleteTable { .. } | WalOp::BulkLoad { .. } => None,
        }
    }
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

/// Leading byte of every v2 entry payload.
///
/// The WAL file itself has no header — it is a bare concatenation of
/// `[len][payload][crc]` records, and both the preallocating sink (which
/// reconstructs its write head by scanning) and the prune-by-rewrite path
/// depend on that. So the format marker lives at the front of each *payload*
/// instead.
///
/// `0xFF` is the byte that makes the two formats provably distinguishable. A
/// v1 (pre-0.3.0) payload opened with the entry's `version: u64` encoded by
/// `bincode::config::standard()`, i.e. a varint whose leading byte is either a
/// literal `0..=250` or a width marker `251..=253` (`254` is u128-only, `255`
/// is not a legal tag at all — see `bincode-2.0.1/src/varint/mod.rs:25-29`).
/// So no v1 payload can begin with `0xFF`, and no v2 payload can be mistaken
/// for a v1 one. This mirrors the `TABLE_MAGIC_V2` decision in
/// [`crate::registry`], and for the same reason: a bare version byte would
/// have collided with a small varint.
const WAL_ENTRY_MAGIC: u8 = 0xFF;

/// On-disk WAL entry format version.
///
/// v1 (pre-0.3.0, implicit — it carried no marker) addressed rows by `u64`
/// id. v2 carries a [`PrimaryKey::KEY_TYPE_ID`](crate::PrimaryKey::KEY_TYPE_ID)
/// tag plus [`PrimaryKey::encode`](crate::PrimaryKey::encode) bytes.
/// There is no compatibility branch: a v1 WAL is rejected by
/// [`check_entry_header`].
const WAL_FORMAT_VERSION: u8 = 2;

/// Validate an entry payload's `[magic][version]` prefix, returning the offset
/// of the first byte after it.
///
/// Called by [`deserialize_entry`] *and* directly by [`scan_wal`], because a
/// version rejection must be a hard error even in tail-tolerant mode: a
/// CRC-valid record is by definition not a torn write into preallocated zeros,
/// so silently treating it as end-of-log would discard a real (v1) log.
fn check_entry_header(data: &[u8]) -> Result<usize> {
    if data.len() < 2 {
        return Err(Error::WalCorrupted(
            "entry payload shorter than the 2-byte format header".into(),
        ));
    }
    if data[0] != WAL_ENTRY_MAGIC {
        return Err(Error::WalCorrupted(format!(
            "WAL entry carries no v{WAL_FORMAT_VERSION} format marker (leading byte 0x{:02X}, \
             expected 0x{WAL_ENTRY_MAGIC:02X}). This is either a pre-0.3.0 WAL or a corrupted \
             one — pre-0.3.0 entries carried no marker at all, so this byte alone cannot tell \
             them apart. If the file predates 0.3.0: 0.3.0 changed the on-disk row-key encoding \
             from fixed u64 ids to encoded primary keys, and pre-0.3.0 checkpoints are rejected \
             too, so checkpointing with the old build does not migrate the data — instead, with \
             the previous UltimaDB version, `Store::recover()` the old directory, read the rows \
             out through a `ReadTx`, and load them into a 0.3.0+ store with `Store::bulk_load` / \
             `Store::bulk_load_batch`. If the file is corrupt: restore from a checkpoint, or \
             delete the WAL and checkpoint files in the persistence directory to start fresh.",
            data[0]
        )));
    }
    if data[1] != WAL_FORMAT_VERSION {
        return Err(Error::WalCorrupted(format!(
            "WAL format version {} is not supported by this build (expected \
             {WAL_FORMAT_VERSION}): the WAL was written by a newer UltimaDB — upgrade the binary \
             to read it.",
            data[1]
        )));
    }
    Ok(2)
}

/// Upper bound on one encoded primary key in a WAL entry.
///
/// The key used to be a `u64` varint — inherently bounded at 9 bytes. It is now
/// a length-prefixed byte string whose length comes straight off disk, so it
/// needs an explicit cap, the same way `op_count` does below. 64 KiB is orders
/// of magnitude above any sane key (a `u64` encodes to 8 bytes; the largest
/// plausible tuple-of-strings key is a few hundred) while keeping a corrupt
/// length from driving a large allocation.
///
/// This mirrors the bounds-checked `take` helpers the registry reader grew for
/// the same reason (task 4 follow-up), so both trust boundaries now validate
/// lengths before acting on them.
///
/// The value is shared with the snapshot wire format rather than restated
/// here — see [`crate::primary_key::MAX_ENCODED_KEY_LEN`] for why the two must
/// not be allowed to drift apart.
const MAX_KEY_LEN: usize = crate::primary_key::MAX_ENCODED_KEY_LEN;

/// Read a key-type tag and length-prefixed encoded primary key at
/// `data[offset..]`, returning the tag, the key, and the number of bytes
/// consumed. The tag is a bincode varint, so it costs one byte for every
/// built-in key type.
///
/// Deliberately does *not* go through `bincode::decode_from_slice::<Vec<u8>>`:
/// that reads the length and allocates in one step, leaving no place to reject
/// an implausible length first. Reading the length separately keeps the
/// validation ahead of the allocation. The length prefix is bincode's own
/// `usize` varint, which bincode documents and implements as a `u64` varint
/// ("usize is being encoded as a u64", `bincode-2.0.1/src/varint/
/// encode_unsigned.rs:109`), so decoding it as `u64` is byte-exact on every
/// platform — pinned by `key_length_prefix_matches_bincode_across_widths`.
fn decode_key(data: &[u8], offset: usize) -> Result<(u32, Vec<u8>, usize)> {
    let config = bincode::config::standard();
    let (key_type, type_read): (u32, usize) = bincode::decode_from_slice(&data[offset..], config)
        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
    let offset = offset + type_read;
    let (len, read): (u64, usize) = bincode::decode_from_slice(&data[offset..], config)
        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
    if len > MAX_KEY_LEN as u64 {
        return Err(Error::WalCorrupted(format!(
            "encoded primary key claims {len} bytes, over the {MAX_KEY_LEN}-byte maximum"
        )));
    }
    let len = len as usize;
    let start = offset + read;
    let end = start
        .checked_add(len)
        .ok_or_else(|| Error::WalCorrupted("encoded primary key length overflows".into()))?;
    if end > data.len() {
        return Err(Error::WalCorrupted(format!(
            "encoded primary key of {len} bytes extends past the end of the entry"
        )));
    }
    Ok((key_type, data[start..end].to_vec(), type_read + read + len))
}

/// Tag bytes for WalOp variants.
const TAG_INSERT: u8 = 1;
const TAG_UPDATE: u8 = 2;
const TAG_DELETE: u8 = 3;
const TAG_CREATE_TABLE: u8 = 4;
const TAG_DELETE_TABLE: u8 = 5;
const TAG_BULK_LOAD: u8 = 6;

/// Write one key-carrying op's `[key_type][len][key bytes]` prefix.
///
/// The cap is enforced **here, on the way out**, and not only in
/// [`decode_key`]. Checking it on read alone meant `commit()` returned `Ok` —
/// telling the caller the transaction was durable — for a key `recover()`
/// could never read back: under `PerEntry` the log became permanently
/// unrecoverable, and under `CoalescedPrealloc` the tail-tolerant scan stopped
/// at the bad record and silently dropped the whole transaction, ordinary
/// co-committed rows included. Refusing at serialization covers all three
/// write modes and every sink at once.
fn serialize_key(buf: &mut Vec<u8>, key_type: u32, key: &[u8]) -> Result<()> {
    let config = bincode::config::standard();
    check_encoded_key_len(key.len(), "WAL entry")?;
    bincode::encode_into_std_write(key_type, buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(key, buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    Ok(())
}

fn serialize_entry(entry: &WalEntry) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    serialize_entry_into(entry, &mut buf)?;
    Ok(buf)
}

/// Append `entry`'s v2 payload to `buf` (no clear — the caller owns framing).
fn serialize_entry_into(entry: &WalEntry, mut buf: &mut Vec<u8>) -> Result<()> {
    let config = bincode::config::standard();

    buf.push(WAL_ENTRY_MAGIC);
    buf.push(WAL_FORMAT_VERSION);
    bincode::encode_into_std_write(entry.version, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(entry.ops.len() as u32, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    for op in &entry.ops {
        match op {
            WalOp::Insert {
                table,
                key_type,
                key,
                data,
            } => {
                buf.push(TAG_INSERT);
                bincode::encode_into_std_write(table.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                serialize_key(buf, *key_type, key)?;
                bincode::encode_into_std_write(data.as_slice(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
            WalOp::Update {
                table,
                key_type,
                key,
                data,
            } => {
                buf.push(TAG_UPDATE);
                bincode::encode_into_std_write(table.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                serialize_key(buf, *key_type, key)?;
                bincode::encode_into_std_write(data.as_slice(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
            }
            WalOp::Delete {
                table,
                key_type,
                key,
            } => {
                buf.push(TAG_DELETE);
                bincode::encode_into_std_write(table.as_str(), &mut buf, config)
                    .map_err(|e| Error::Persistence(e.to_string()))?;
                serialize_key(buf, *key_type, key)?;
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

    Ok(())
}

/// A WAL entry already serialized to its on-disk `[len][payload][crc]` frame.
///
/// Built once on the *committing* thread ([`frame_entry_into`]) and handed to
/// the WAL thread, which just writes `bytes`. This keeps every per-op
/// allocation (record bytes, keys, op vectors) allocated *and* freed on the
/// committer — the cross-thread free of that chain was measured at ~2–2.5 µs
/// per eventual-tier commit under glibc (see
/// `docs/benchmarks/ycsb-eventual-write-decomposition-2026-08-02.md`).
pub(crate) struct FramedEntry {
    pub(crate) version: u64,
    pub(crate) bytes: Vec<u8>,
}

impl FramedEntry {
    #[cfg(test)]
    pub(crate) fn new(entry: &WalEntry) -> Result<Self> {
        let mut bytes = Vec::new();
        frame_entry_into(entry, &mut bytes)?;
        Ok(FramedEntry {
            version: entry.version,
            bytes,
        })
    }
}

/// Bounded pool recycling [`FramedEntry`] buffers from the WAL thread back to
/// committers. Without it, every commit's framed bytes are allocated on the
/// committing thread and freed on the WAL thread — a cross-thread pattern
/// glibc malloc handles poorly (measured ~2–2.5 µs/commit on the eventual
/// tier; a mimalloc A/B recovered 27–29% of YCSB A/F, see the 2026-08-02
/// decomposition doc). Recycling keeps the buffer's whole lifecycle
/// effectively committer-owned.
pub(crate) struct BufPool {
    bufs: Mutex<Vec<Vec<u8>>>,
}

/// Max buffers retained; beyond this, returned buffers are simply dropped.
/// Sized for bursts of in-flight eventual commits, not for correctness.
const BUF_POOL_CAP: usize = 64;

/// A returned buffer whose capacity exceeds this is dropped instead of pooled,
/// so one jumbo entry (bulk-load marker, giant record) can't pin memory.
const BUF_POOL_MAX_RETAIN: usize = 1 << 20;

impl BufPool {
    fn new() -> Arc<Self> {
        Arc::new(BufPool {
            bufs: Mutex::new(Vec::new()),
        })
    }

    /// Pop a recycled buffer (cleared by `frame_entry_into`) or a fresh one.
    fn take(&self) -> Vec<u8> {
        self.bufs.lock().pop().unwrap_or_default()
    }

    fn put(&self, buf: Vec<u8>) {
        if buf.capacity() > BUF_POOL_MAX_RETAIN {
            return;
        }
        let mut bufs = self.bufs.lock();
        if bufs.len() < BUF_POOL_CAP {
            bufs.push(buf);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.bufs.lock().len()
    }
}

fn deserialize_entry(data: &[u8]) -> Result<WalEntry> {
    let config = bincode::config::standard();
    let mut offset = check_entry_header(data)?;

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
                let (key_type, key, read) = decode_key(data, offset)?;
                offset += read;
                let (blob, read): (Vec<u8>, _) =
                    bincode::decode_from_slice(&data[offset..], config)
                        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                if tag == TAG_INSERT {
                    ops.push(WalOp::Insert {
                        table,
                        key_type,
                        key,
                        data: blob,
                    });
                } else {
                    ops.push(WalOp::Update {
                        table,
                        key_type,
                        key,
                        data: blob,
                    });
                }
            }
            TAG_DELETE => {
                let (table, read): (String, _) =
                    bincode::decode_from_slice(&data[offset..], config)
                        .map_err(|e| Error::WalCorrupted(e.to_string()))?;
                offset += read;
                let (key_type, key, read) = decode_key(data, offset)?;
                offset += read;
                ops.push(WalOp::Delete {
                    table,
                    key_type,
                    key,
                });
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
    let mut buf = Vec::new();
    frame_entry_into(entry, &mut buf)?;
    Ok(buf)
}

/// Build the on-disk `[len][payload][crc]` record for `entry` in `buf`,
/// clearing it first — one serialization pass, no intermediate payload Vec,
/// and a cleared `buf` keeps its capacity so pooled buffers are reused.
fn frame_entry_into(entry: &WalEntry, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    buf.extend_from_slice(&[0u8; 4]); // len placeholder, patched below
    serialize_entry_into(entry, buf)?;
    let len = (buf.len() - 4) as u32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    let checksum = crc32(&buf[4..]);
    buf.extend_from_slice(&checksum.to_le_bytes());
    Ok(())
}

#[cfg(test)]
fn write_entry_to_file(file: &mut File, entry: &WalEntry) -> Result<()> {
    file.write_all(&frame_entry(entry)?)
        .map_err(|e| Error::Persistence(e.to_string()))
}

/// Physically zero-fill `[from, to)` with real writes (NOT sparse `set_len`,
/// so ext4 marks the extents *written*), then `sync_all` once so the size and
/// allocation are durable before any record is written into the region. The
/// WAL counterpart of `ultima_journal`'s `SegmentFile::preallocate_to`. No-op
/// when `to <= from`.
///
/// On error the file may be left physically longer than `from` with that
/// extension un-`sync_all`'d, so callers that care about the durability
/// invariant must roll the size back — see `PreallocFileSink::sync`.
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
/// detection for the non-preallocated path). An unreadable *format version* in
/// a CRC-valid record is a hard error either way — see [`check_entry_header`].
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
        // Format-version rejection is a hard error in BOTH modes. The CRC just
        // verified, so this record is not a torn write into preallocated zeros
        // — it is a real record in a format this build cannot read (a v1 WAL).
        // Treating it as end-of-log would silently discard the whole log.
        check_entry_header(data)?;
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
        entry: FramedEntry,
        durability: Arc<WalDurability>,
        poison: Arc<WalPoison>,
        pool: Arc<BufPool>,
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
            SyncWaiter::InlineSync { sink, entry, durability, poison, pool } => {
                poison.check()?;
                let version = entry.version;
                let mut s = sink.lock();
                let res: Result<()> = (|| {
                    s.append(&entry)?;
                    s.sync()
                })();
                match res {
                    Ok(()) => {
                        pool.put(entry.bytes);
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
    fn append(&mut self, entry: &FramedEntry) -> Result<()>;
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

impl WalSinkKind {
    /// Whether a CRC mismatch in this sink's WAL is end-of-log rather than
    /// corruption — i.e. the `tail_tolerant` argument to [`scan_wal`].
    ///
    /// **This is the single source of truth for that policy.** It is consumed
    /// both by the sink reconstructing its own write head on open and by
    /// `Store::recover` deciding how to read the same file; encoding it twice
    /// let the two drift apart in principle (issue #24). The `match` is
    /// deliberately exhaustive with no wildcard arm, so adding a sink is a
    /// compile error until its tolerance is stated here.
    ///
    /// A presizing sink writes into a region of known zeros, so a torn write
    /// there is indistinguishable from never-written space and must be treated
    /// as end-of-log. An append-only sink has no such region: a bad CRC is real
    /// corruption and must be loud.
    pub(crate) fn tail_tolerant(self) -> bool {
        match self {
            // Append-only: no preallocated zero region, so a bad CRC is corruption.
            WalSinkKind::FsWrite | WalSinkKind::Coalesced | WalSinkKind::BufferedFile => false,
            // Presized: a torn write into preallocated zeros is end-of-log.
            WalSinkKind::CoalescedPrealloc => true,
            // Presized via `set_len` (sparse). Bench-only, and `MmapSink::open`
            // takes its write head from `metadata().len()` without scanning at
            // all — so this arm states the policy recovery would need, not one
            // the sink itself currently applies.
            #[cfg(feature = "bench-internals")]
            WalSinkKind::Mmap => true,
            // Append-only, like the other file sinks.
            #[cfg(all(target_os = "linux", feature = "wal-iouring"))]
            WalSinkKind::IoUring => false,
        }
    }
}

impl crate::persistence::WalWrite {
    /// The sink this durability setting selects. Exhaustive with no wildcard so
    /// a new `WalWrite` variant cannot silently inherit another's sink — the
    /// other half of the #24 coupling.
    pub(crate) fn sink_kind(self) -> WalSinkKind {
        match self {
            crate::persistence::WalWrite::PerEntry => WalSinkKind::FsWrite,
            crate::persistence::WalWrite::Coalesced => WalSinkKind::Coalesced,
            crate::persistence::WalWrite::CoalescedPrealloc => WalSinkKind::CoalescedPrealloc,
        }
    }
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
        reject_unreadable_wal(&wal_path)?;
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

/// Refuse to *open* a WAL whose first record this build cannot read.
///
/// The append-mode sinks ([`FileSink`], [`BufferedFileSink`]) never read the
/// file they open. Without this check, a store pointed at a pre-0.3.0
/// directory constructs cleanly, accepts commits, and returns `Ok` from a
/// `Durability::Consistent` `commit()` — telling the caller the data is
/// durable — while appending v2 records behind a v1 prefix. The next
/// `recover()` then fails permanently, and the only remedy the error can offer
/// is deleting the WAL, which destroys exactly the commits that were
/// acknowledged. Failing at open is the honest behavior: the operator finds out
/// before they think they have written data.
///
/// [`PreallocFileSink::open_with_chunk`] already refuses, because it scans the
/// file to reconstruct its write head; this closes the same gap for the two
/// sinks that do not scan.
///
/// Only the first record's header is inspected — a full scan on every store
/// construction would cost O(WAL), and one record is enough: a WAL file is
/// written by a single build, so its records are homogeneous. A missing,
/// empty, or sub-record-sized file is accepted (there is nothing to misread);
/// a first record that is present but damaged is reported by the same
/// "pre-0.3.0 or corrupt" message, which is accurate either way — and these
/// sinks recover strictly, so such a file could not have recovered regardless.
fn reject_unreadable_wal(path: &Path) -> Result<()> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::Persistence(e.to_string())),
    };
    // 4-byte frame length + the 2-byte payload header.
    let mut head = [0u8; 6];
    match file.read_exact(&mut head) {
        Ok(()) => {}
        // Shorter than one header: an empty or freshly created WAL.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
        Err(e) => return Err(Error::Persistence(e.to_string())),
    }
    if u32::from_le_bytes(head[0..4].try_into().unwrap()) == 0 {
        return Ok(()); // zero len-prefix: empty / preallocated log
    }
    check_entry_header(&head[4..]).map(|_| ())
}

impl WalSink for FileSink {
    fn append(&mut self, entry: &FramedEntry) -> Result<()> {
        self.file
            .write_all(&entry.bytes)
            .map_err(|e| Error::Persistence(e.to_string()))
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
        reject_unreadable_wal(&wal_path)?;
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
    fn append(&mut self, entry: &FramedEntry) -> Result<()> {
        self.buf.extend_from_slice(&entry.bytes);
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
        // Reconstruct the write head; a torn tail into preallocated zeros is
        // end-of-log, not corruption. The policy comes from `tail_tolerant`
        // rather than a literal here, so this and `Store::recover` cannot
        // disagree about how to read the same file (issue #24).
        let (_entries, write_head) = scan_wal(&path, WalSinkKind::CoalescedPrealloc.tail_tolerant())?;
        let capacity = file.metadata().map_err(|e| Error::Persistence(e.to_string()))?.len();
        Ok(PreallocFileSink { file, path, buf: Vec::new(), write_head, capacity, chunk })
    }
}

impl WalSink for PreallocFileSink {
    fn append(&mut self, entry: &FramedEntry) -> Result<()> {
        self.buf.extend_from_slice(&entry.bytes);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        if !self.buf.is_empty() {
            let need = self.write_head + self.buf.len() as u64;
            if need > self.capacity {
                // Extend by whole chunks to cover `need`; sync_all (size change).
                let new_cap = need.div_ceil(self.chunk) * self.chunk;
                if let Err(e) = preallocate_to(&mut self.file, self.capacity, new_cap) {
                    // The zero-fill died partway (ENOSPC is the realistic
                    // trigger), so the file is physically longer than
                    // `capacity` but that extension never reached `sync_all`.
                    // Roll it back, so the on-disk size stays one we know is
                    // durable: otherwise a restart adopts the torn extension
                    // via `metadata().len()` (see `open_with_chunk`) and
                    // records land in a region whose allocation only
                    // `sync_data` ever covers — the exact dependency
                    // preallocation exists to remove (task37 §4 invariant 2).
                    // Truncating frees space, so it still works under ENOSPC;
                    // if it fails anyway, the caller needs the original error.
                    let _ = self.file.set_len(self.capacity);
                    let _ = self.file.sync_all();
                    return Err(e);
                }
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
    fn append(&mut self, entry: &FramedEntry) -> Result<()> {
        self.ensure_capacity(entry.bytes.len())?;
        self.map[self.write_head..self.write_head + entry.bytes.len()]
            .copy_from_slice(&entry.bytes);
        self.write_head += entry.bytes.len();
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
    fn append(&mut self, entry: &FramedEntry) -> Result<()> {
        self.buf.extend_from_slice(&entry.bytes);
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
    Entry(FramedEntry),
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
    /// Recycled framed-entry buffers (see [`BufPool`]).
    pool: Arc<BufPool>,
    /// When `Some`, this handle has NO background thread —
    /// `write()` stages an `InlineSync` waiter (no I/O yet); the caller drives
    /// the actual append+fsync by calling `wait()` off the store lock. This
    /// eliminates the enqueue→wake-writer→fsync→wake-waiter handoff
    /// (~20–35µs/commit) that only pays off when commits can batch. Wired for
    /// `SingleWriter + Consistent` (serial commits never batch). See
    /// [the task38 design notes](https://github.com/PeterKnego/ultima_db/blob/main/docs/tasks/task38_wal_inline_fsync.md).
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

        let pool = BufPool::new();
        let handle = spawn_wal_thread(
            sink,
            rx,
            bg_in_flight,
            bg_sync_state,
            bg_poison,
            bg_durability,
            Arc::clone(&pool),
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
            pool,
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
            pool: BufPool::new(),
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

    /// Number of recycled framed-entry buffers currently pooled.
    #[cfg(test)]
    pub(crate) fn pool_len(&self) -> usize {
        self.pool.len()
    }

    /// Submit a WAL entry to the background thread.
    ///
    /// - **Consistent**: returns `SyncWaiter::WaitForEpoch` — caller must
    ///   call `wait()` outside the store lock to block until fsync.
    /// - **Eventual**: returns `SyncWaiter::Done` — no wait needed.
    pub fn write(&self, entry: WalEntry) -> Result<SyncWaiter> {
        // Frame here, on the committing thread: one serialization pass, and
        // the entry's whole ops chain (record bytes, keys, vectors) is freed
        // on this thread when `entry` drops at the end of this call, instead
        // of crossing to the WAL thread — cross-thread frees of that chain
        // cost ~2–2.5 µs/commit under glibc.
        let mut bytes = self.pool.take();
        frame_entry_into(&entry, &mut bytes)?;
        let framed = FramedEntry {
            version: entry.version,
            bytes,
        };
        // Inline (no bg thread): do NO I/O here (this runs under store_inner).
        // Return a waiter; the committer does append+fsync off-lock in wait().
        if let Some(sink) = &self.sync_sink {
            self.poison.check()?;
            return Ok(SyncWaiter::InlineSync {
                sink: Arc::clone(sink),
                entry: framed,
                durability: Arc::clone(&self.durability),
                poison: Arc::clone(&self.poison),
                pool: Arc::clone(&self.pool),
            });
        }
        let sender = self.sender.as_ref().expect("WalHandle used after drop");
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        sender
            .send(WalMsg::Entry(framed))
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
    pool: Arc<BufPool>,
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

            // Recycle the batch's buffers before publishing durability, so a
            // committer that observes the watermark also sees the pool
            // refilled (keeps tests deterministic; harmless otherwise).
            for e in batch {
                pool.put(e.bytes);
            }

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
    use crate::{PrimaryKey, Store};

    /// Encoded-key helper for fixtures that used to spell a bare `u64` id.
    /// `u64::encode` is big-endian, so this is exactly what the store writes.
    fn k(id: u64) -> Vec<u8> {
        id.encode()
    }

    /// The key-type tag every `u64`-keyed fixture carries, spelled once.
    const KT: u32 = <u64 as PrimaryKey>::KEY_TYPE_ID;

    fn one_op_entry(version: u64) -> WalEntry {
        WalEntry {
            version,
            ops: vec![WalOp::Insert {
                table: "t".into(),
                key_type: KT,
                key: k(version),
                data: vec![7; 256],
            }],
        }
    }

    #[test]
    fn eventual_write_recycles_framed_buffers_through_the_pool() {
        let dir = crate::test_scratch::scratch_dir();
        let handle = WalHandle::with_sink_kind(
            dir.path(),
            false,
            Arc::new(WalPoison::new()),
            WalSinkKind::Coalesced,
        )
        .unwrap();
        assert_eq!(handle.pool_len(), 0, "pool starts empty");

        handle.write(one_op_entry(1)).unwrap();
        handle.durability().wait(1).unwrap();
        assert_eq!(handle.pool_len(), 1, "flushed buffer returned to the pool");

        handle.write(one_op_entry(2)).unwrap();
        handle.durability().wait(2).unwrap();
        assert_eq!(
            handle.pool_len(),
            1,
            "second write reused the pooled buffer instead of allocating"
        );
    }

    #[test]
    fn inline_write_recycles_framed_buffers_through_the_pool() {
        let dir = crate::test_scratch::scratch_dir();
        let handle = WalHandle::with_sink_kind_inline(
            dir.path(),
            true,
            Arc::new(WalPoison::new()),
            WalSinkKind::Coalesced,
        )
        .unwrap();
        assert_eq!(handle.pool_len(), 0, "pool starts empty");

        handle.write(one_op_entry(1)).unwrap().wait().unwrap();
        assert_eq!(handle.pool_len(), 1, "driven waiter returned its buffer");

        handle.write(one_op_entry(2)).unwrap().wait().unwrap();
        assert_eq!(handle.pool_len(), 1, "buffer was reused, not re-allocated");
    }

    #[test]
    fn frame_entry_into_matches_frame_entry_and_reuses_the_buffer() {
        let entry = WalEntry {
            version: 7,
            ops: vec![
                WalOp::Insert {
                    table: "t".into(),
                    key_type: KT,
                    key: k(1),
                    data: vec![9; 300],
                },
                WalOp::Delete {
                    table: "t".into(),
                    key_type: KT,
                    key: k(2),
                },
            ],
        };
        let expected = frame_entry(&entry).unwrap();

        // Start from a dirty, over-capacity buffer: the into-variant must
        // clear it, produce identical bytes, and keep the capacity.
        let mut buf = vec![0xAA_u8; 4096];
        let cap_before = buf.capacity();
        frame_entry_into(&entry, &mut buf).unwrap();
        assert_eq!(buf, expected);
        assert!(buf.capacity() >= cap_before, "buffer capacity was not reused");
    }

    /// Frame a `u64` key the way [`serialize_key`] does, for the `decode_key`
    /// tests that assemble a key field by hand.
    fn framed_key(key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_key(&mut buf, KT, key).unwrap();
        buf
    }

    // -----------------------------------------------------------------------
    // Bounded key reads
    // -----------------------------------------------------------------------

    /// `decode_key` reads the length prefix itself rather than letting bincode
    /// read-and-allocate in one step, so it must agree with bincode's writer at
    /// every varint width — including the boundaries where the encoding changes
    /// shape (250 -> literal byte, 251 -> `U16_BYTE` + 2, 65536 -> `U32_BYTE` + 4).
    #[test]
    fn key_length_prefix_matches_bincode_across_widths() {
        let config = bincode::config::standard();
        for len in [0usize, 1, 8, 250, 251, 252, 1000, 65_535, MAX_KEY_LEN] {
            let key: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let enc = framed_key(&key);
            // The length prefix bincode would write, for the width assertion.
            let bincode_len = bincode::encode_to_vec(key.as_slice(), config).unwrap().len();
            let tag_len = bincode::encode_to_vec(KT, config).unwrap().len();
            let (got_type, got, read) =
                decode_key(&enc, 0).unwrap_or_else(|e| panic!("len {len} rejected: {e}"));
            assert_eq!(got_type, KT, "len {len}: key type tag differs");
            assert_eq!(got, key, "len {len}: bytes differ");
            assert_eq!(read, enc.len(), "len {len}: consumed the wrong span");
            assert_eq!(
                read,
                tag_len + bincode_len,
                "len {len}: the field is [key type tag][bincode-framed key]"
            );
            // And the same read offset by a prefix, as it is used in practice.
            let mut framed = vec![0xAAu8; 3];
            framed.extend_from_slice(&enc);
            let (got_type2, got2, read2) = decode_key(&framed, 3).unwrap();
            assert_eq!(
                (got_type2, got2, read2),
                (KT, key, read),
                "len {len}: offset read differs"
            );
        }
    }

    /// A corrupt length must be refused *before* it can drive an allocation.
    /// The claim here is 3 GB inside an 11-byte buffer.
    #[test]
    fn an_over_long_key_length_is_rejected() {
        let mut evil = vec![KT as u8]; // key type tag (a one-byte varint)
        evil.push(253u8); // bincode U64_BYTE discriminant
        evil.extend_from_slice(&3_000_000_000u64.to_le_bytes());
        evil.extend_from_slice(&[7, 7]);
        let err = decode_key(&evil, 0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("3000000000"), "message was: {msg}");
        assert!(msg.contains(&MAX_KEY_LEN.to_string()), "message was: {msg}");
    }

    /// A length that is plausible but runs past the end of the entry is a
    /// truncation, and is reported as one rather than panicking on the slice.
    #[test]
    fn a_key_running_past_the_end_of_the_entry_is_rejected() {
        let mut short = framed_key(&[0u8; 100]);
        short.truncate(5); // tag + 1 length byte + 3 of the 100 key bytes
        let err = decode_key(&short, 0).unwrap_err();
        assert!(
            err.to_string().contains("past the end"),
            "message was: {err}"
        );
    }

    /// A key at the cap survives a full entry round trip; one byte over is
    /// refused **by the writer**.
    ///
    /// The writer is where it has to be caught. Enforcing the cap only on read
    /// meant `serialize_entry` produced a record no reader would ever accept:
    /// `commit()` returned `Ok` — an acknowledged, supposedly durable
    /// transaction — and then `recover()` either failed permanently
    /// (`PerEntry`) or, under the tail-tolerant `CoalescedPrealloc` scan the
    /// `standalone_fast` preset uses, stopped at the bad record and silently
    /// dropped the whole transaction, ordinary co-committed rows included.
    #[test]
    fn a_key_at_the_cap_roundtrips_and_one_over_is_refused() {
        let at_cap = vec![0x5Au8; MAX_KEY_LEN];
        let entry = WalEntry {
            version: 1,
            ops: vec![WalOp::Insert {
                table: "t".into(),
                key_type: KT,
                key: at_cap.clone(),
                data: vec![1],
            }],
        };
        let bytes = serialize_entry(&entry).unwrap();
        let back = deserialize_entry(&bytes).unwrap();
        assert!(matches!(&back.ops[0], WalOp::Insert { key, .. } if *key == at_cap));

        // One byte over, in each of the three key-carrying variants: the
        // choke point covers all of them, and all sinks with them.
        let too_long = vec![0x5Au8; MAX_KEY_LEN + 1];
        let over = [
            WalOp::Insert {
                table: "t".into(),
                key_type: KT,
                key: too_long.clone(),
                data: vec![1],
            },
            WalOp::Update {
                table: "t".into(),
                key_type: KT,
                key: too_long.clone(),
                data: vec![1],
            },
            WalOp::Delete {
                table: "t".into(),
                key_type: KT,
                key: too_long.clone(),
            },
        ];
        for op in over {
            let entry = WalEntry {
                version: 1,
                ops: vec![op],
            };
            let err = serialize_entry(&entry).unwrap_err();
            assert!(err.to_string().contains("maximum"), "message was: {err}");
        }
    }

    /// The key-type tag rides along in every key-carrying op and survives the
    /// round trip. Without it a table reopened under a different `K` recovers
    /// `Ok` with silently reinterpreted keys — see `Store::recover`, which
    /// compares this tag against the registry's.
    #[test]
    fn the_key_type_tag_round_trips_on_every_key_carrying_op() {
        let entry = WalEntry {
            version: 7,
            ops: vec![
                WalOp::Insert {
                    table: "t".into(),
                    key_type: <String as PrimaryKey>::KEY_TYPE_ID,
                    key: b"alice".to_vec(),
                    data: vec![1],
                },
                WalOp::Update {
                    table: "t".into(),
                    key_type: <String as PrimaryKey>::KEY_TYPE_ID,
                    key: b"alice".to_vec(),
                    data: vec![2],
                },
                WalOp::Delete {
                    table: "t".into(),
                    key_type: <String as PrimaryKey>::KEY_TYPE_ID,
                    key: b"alice".to_vec(),
                },
                WalOp::Insert {
                    table: "u".into(),
                    key_type: KT,
                    key: k(1),
                    data: vec![3],
                },
                // Key-free ops carry no tag.
                WalOp::DeleteTable { name: "v".into() },
            ],
        };
        let back = deserialize_entry(&serialize_entry(&entry).unwrap()).unwrap();
        let tags: Vec<Option<u32>> = back.ops.iter().map(WalOp::key_type).collect();
        let s = Some(<String as PrimaryKey>::KEY_TYPE_ID);
        assert_eq!(tags, vec![s, s, s, Some(KT), None]);
        // The two tags must actually differ, or the test proves nothing.
        assert_ne!(<String as PrimaryKey>::KEY_TYPE_ID, KT);
    }

    // -----------------------------------------------------------------------
    // v1 (pre-0.3.0) format rejection
    // -----------------------------------------------------------------------

    /// Byte-for-byte reproduction of the pre-0.3.0 `serialize_entry`: no format
    /// header, and row keys written as a bincode varint `u64` id. This is the
    /// artifact an operator has on disk when they upgrade, so the rejection
    /// tests run against the real thing rather than a hand-waved stand-in.
    fn v1_serialize_entry(version: u64, ops: &[(u8, &str, u64, &[u8])]) -> Vec<u8> {
        let config = bincode::config::standard();
        let mut buf = Vec::new();
        bincode::encode_into_std_write(version, &mut buf, config).unwrap();
        bincode::encode_into_std_write(ops.len() as u32, &mut buf, config).unwrap();
        for (tag, table, id, data) in ops {
            buf.push(*tag);
            bincode::encode_into_std_write(*table, &mut buf, config).unwrap();
            bincode::encode_into_std_write(*id, &mut buf, config).unwrap();
            if *tag != TAG_DELETE {
                bincode::encode_into_std_write(*data, &mut buf, config).unwrap();
            }
        }
        buf
    }

    /// Frame a raw payload the way the v1 (and v2) file format does. The
    /// framing itself did not change, so a v1 file is a well-formed, CRC-valid
    /// record sequence — which is exactly why the payload must self-identify.
    fn v1_frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
        out.extend_from_slice(&crc32(payload).to_le_bytes());
        out
    }

    /// The whole rejection scheme rests on this: bincode's varint (which is how
    /// a v1 payload's leading `version` field was written) never emits `0xFF`
    /// as a first byte. Values `0..=250` are literal; `251..=253` are the u16/
    /// u32/u64 width markers; `254` is u128-only and `255` is not a legal tag.
    /// So no v1 payload can be mistaken for a v2 one.
    #[test]
    fn a_v1_payload_can_never_begin_with_the_v2_magic() {
        let config = bincode::config::standard();
        let interesting: Vec<u64> = vec![
            0,
            1,
            2,
            249,
            250,
            251,
            252,
            253,
            254,
            255,
            u16::MAX as u64 - 1,
            u16::MAX as u64,
            u32::MAX as u64 - 1,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX - 1,
            u64::MAX,
        ];
        for v in interesting.into_iter().chain(0..5_000u64) {
            let enc = bincode::encode_to_vec(v, config).unwrap();
            assert_ne!(
                enc[0], WAL_ENTRY_MAGIC,
                "bincode varint for {v} starts with the v2 magic byte"
            );
        }
    }

    /// A genuine v1 WAL file must be rejected with an actionable error, not
    /// silently misread. `read_wal` (strict) is the recovery path for the
    /// PerEntry/Coalesced sinks.
    #[test]
    fn strict_scan_rejects_a_genuine_v1_wal() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);
        let mut bytes = v1_frame(&v1_serialize_entry(1, &[(TAG_INSERT, "users", 1, &[10, 20])]));
        bytes.extend_from_slice(&v1_frame(&v1_serialize_entry(
            2,
            &[(TAG_DELETE, "users", 1, &[])],
        )));
        std::fs::write(&path, &bytes).unwrap();

        let err = read_wal(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no v2 format marker"), "message was: {msg}");
        // The leading byte alone cannot distinguish a v1 WAL from a corrupt
        // one, so the message must offer both readings and a remedy for each
        // — not present the pre-0.3.0 migration as established fact.
        assert!(msg.contains("pre-0.3.0"), "message was: {msg}");
        assert!(msg.contains("corrupt"), "message was: {msg}");
        assert!(msg.contains("Store::bulk_load"), "message was: {msg}");
    }

    /// The dangerous case. The preallocating sink scans tail-tolerantly, where
    /// an undecodable record means "end of log". A v1 record is CRC-valid, so
    /// it is NOT a torn tail — silently stopping there would report an empty
    /// WAL and throw away every committed transaction. Both the recovery scan
    /// and `PreallocFileSink::open` must hard-error instead.
    #[test]
    fn tolerant_scan_rejects_a_genuine_v1_wal_instead_of_reporting_end_of_log() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);
        let mut bytes = v1_frame(&v1_serialize_entry(1, &[(TAG_INSERT, "users", 1, &[10, 20])]));
        bytes.extend_from_slice(&[0u8; 4096]); // preallocated zero tail
        std::fs::write(&path, &bytes).unwrap();

        let err = scan_wal(&path, true).unwrap_err();
        assert!(
            err.to_string().contains("no v2 format marker"),
            "message was: {err}"
        );
        assert!(PreallocFileSink::open_with_chunk(dir.path(), 4096).is_err());
    }

    /// The concrete silent misread the marker prevents.
    ///
    /// Without a format marker, this exact v1 record decodes with **no error**
    /// into `Insert { table: "t", key: [4], data: [65, 66, 67] }` — wrong key,
    /// wrong payload, no complaint. (Verified during development by making
    /// `check_entry_header` a no-op and printing the result.) The v1 `id`
    /// varint is consumed as the v2 key's *length* prefix, so a v1 record whose
    /// following bytes happen to line up round-trips into a plausible-looking
    /// lie. It must be rejected.
    #[test]
    fn a_v1_record_that_would_otherwise_decode_cleanly_is_rejected() {
        let payload = v1_serialize_entry(1, &[(TAG_INSERT, "t", 1, &[3, 65, 66, 67])]);
        assert_eq!(
            payload,
            vec![0x01, 0x01, 0x01, 0x01, b't', 0x01, 0x04, 0x03, 65, 66, 67],
            "fixture no longer reproduces the misreading record"
        );
        let err = deserialize_entry(&payload).unwrap_err();
        assert!(
            err.to_string().contains("no v2 format marker"),
            "message was: {err}"
        );
    }

    /// A v1 payload whose leading varint byte collides with the *version*
    /// number (2) is the trap a bare version byte would have fallen into: it
    /// must still be rejected, because the magic comes first.
    #[test]
    fn a_v1_payload_whose_first_byte_is_two_is_still_rejected() {
        let payload = v1_serialize_entry(2, &[(TAG_INSERT, "t", 1, &[9])]);
        assert_eq!(
            payload[0], WAL_FORMAT_VERSION,
            "fixture no longer exercises the version-byte collision"
        );
        let err = check_entry_header(&payload).unwrap_err();
        assert!(
            err.to_string().contains("no v2 format marker"),
            "message was: {err}"
        );
    }

    /// A WAL written by a *newer* build is rejected too, naming its version.
    #[test]
    fn a_newer_format_version_is_rejected() {
        let mut payload = serialize_entry(&WalEntry {
            version: 1,
            ops: vec![WalOp::CreateTable { name: "t".into() }],
        })
        .unwrap();
        payload[1] = WAL_FORMAT_VERSION + 1;
        let err = check_entry_header(&payload).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("format version 3"), "message was: {msg}");
        assert!(msg.contains("newer UltimaDB"), "message was: {msg}");
    }

    /// Pins the v2 payload prefix so a later edit that shifts it breaks a test
    /// naming the field rather than silently changing the on-disk format.
    #[test]
    fn v2_payload_starts_with_magic_then_version() {
        let payload = serialize_entry(&WalEntry {
            version: 7,
            ops: vec![WalOp::CreateTable { name: "t".into() }],
        })
        .unwrap();
        assert_eq!(payload[0], 0xFF);
        assert_eq!(payload[1], 2);
        // The entry version follows immediately, still a bincode varint.
        assert_eq!(payload[2], 7);
        assert_eq!(check_entry_header(&payload).unwrap(), 2);
    }

    /// A payload too short to hold the header errors rather than panicking on
    /// an out-of-range index.
    #[test]
    fn a_truncated_header_errors_not_panics() {
        assert!(check_entry_header(&[]).is_err());
        assert!(check_entry_header(&[WAL_ENTRY_MAGIC]).is_err());
        assert!(deserialize_entry(&[WAL_ENTRY_MAGIC]).is_err());
    }

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
        let e1 = WalEntry { version: 1, ops: vec![WalOp::Insert { table: "t".into(), key_type: KT, key: k(1), data: vec![1, 2, 3] }] };
        let e2 = WalEntry { version: 2, ops: vec![WalOp::Delete { table: "t".into(), key_type: KT, key: k(1) }] };

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
        assert_eq!(wtx.wal_ops.borrow().len(), 4);
        assert!(matches!(&wtx.wal_ops.borrow()[0], WalOp::Insert { table, key, .. } if table == "users" && *key == k(1)));
        assert!(matches!(&wtx.wal_ops.borrow()[1], WalOp::Insert { table, key, .. } if table == "users" && *key == k(2)));
        assert!(matches!(&wtx.wal_ops.borrow()[2], WalOp::Update { table, key, .. } if table == "users" && *key == k(1)));
        assert!(matches!(&wtx.wal_ops.borrow()[3], WalOp::Delete { table, key, .. } if table == "users" && *key == k(2)));
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
        assert_eq!(wtx.wal_ops.borrow().len(), 1);
        assert!(matches!(&wtx.wal_ops.borrow()[0], WalOp::DeleteTable { name } if name == "users"));
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
        assert_eq!(wtx.wal_ops.borrow().len(), 4); // 2 inserts + 2 deletes
    }

    #[test]
    fn wal_entry_serialize_deserialize_roundtrip() {
        let entry = WalEntry {
            version: 42,
            ops: vec![
                WalOp::Insert {
                    table: "users".into(),
                    key_type: KT,
                    key: k(1),
                    data: vec![1, 2, 3],
                },
                WalOp::Update {
                    table: "users".into(),
                    key_type: KT,
                    key: k(1),
                    data: vec![4, 5, 6],
                },
                WalOp::Delete {
                    table: "users".into(),
                    key_type: KT,
                    key: k(1),
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
        assert!(matches!(&recovered.ops[0], WalOp::Insert { key, .. } if *key == k(1)));
        assert!(matches!(&recovered.ops[1], WalOp::Update { key, .. } if *key == k(1)));
        assert!(matches!(&recovered.ops[2], WalOp::Delete { key, .. } if *key == k(1)));
    }

    /// The point of the v2 format: a key that is neither 8 bytes nor a valid
    /// varint round-trips byte-for-byte, including an embedded NUL and a
    /// leading 0xFF (which is the entry magic — it must not confuse framing).
    #[test]
    fn variable_length_keys_roundtrip_through_the_wal_file() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);
        let keys: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"alice@example.com".to_vec(),
            vec![0x00, 0xFF, 0x00],
            vec![0xFF; 300],
        ];
        let entry = WalEntry {
            version: 3,
            ops: keys
                .iter()
                .map(|key| WalOp::Insert {
                    table: "emails".into(),
                    key_type: KT,
                    key: key.clone(),
                    data: vec![7],
                })
                .collect(),
        };
        std::fs::write(&path, frame_entry(&entry).unwrap()).unwrap();

        let read = read_wal(&path).unwrap();
        assert_eq!(read.len(), 1);
        let got: Vec<Vec<u8>> = read[0]
            .ops
            .iter()
            .map(|op| match op {
                WalOp::Insert { key, .. } => key.clone(),
                other => panic!("unexpected op: {other:?}"),
            })
            .collect();
        assert_eq!(got, keys);
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
                    key_type: KT,
                    key: k(1),
                    data: vec![10],
                }],
            };
            let e2 = WalEntry {
                version: 2,
                ops: vec![WalOp::Delete {
                    table: "t".into(),
                    key_type: KT,
                    key: k(1),
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
                    key_type: KT,
                    key: k(1),
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
            // Craft a minimal entry: v2 header, version=1, op_count=1,
            // tag=0xFF (invalid).
            let config = bincode::config::standard();
            let mut data = vec![WAL_ENTRY_MAGIC, WAL_FORMAT_VERSION];
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
            let mut data = vec![WAL_ENTRY_MAGIC, WAL_FORMAT_VERSION];
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
        fn append(&mut self, _entry: &FramedEntry) -> Result<()> {
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
                sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), key_type: KT, key: k(v), data: vec![v as u8; 32] }] }).unwrap()).unwrap();
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
                sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), key_type: KT, key: k(v), data: vec![v as u8; 32] }] }).unwrap()).unwrap();
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
                sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), key_type: KT, key: k(v), data: vec![v as u8; 32] }] }).unwrap()).unwrap();
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
                sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::Insert { table: "t".into(), key_type: KT, key: k(v), data: vec![0u8; 16 * 1024] }] }).unwrap()).unwrap();
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
                sink.append(&FramedEntry::new(&WalEntry {
                    version: v,
                    ops: vec![WalOp::Insert {
                        table: "t".into(),
                        key_type: KT,
                        key: k(v),
                        data: vec![v as u8; 32],
                    }],
                }).unwrap())
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
        sink.append(&FramedEntry::new(&WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap()).unwrap();
        sink.append(&FramedEntry::new(&WalEntry { version: 2, ops: vec![WalOp::DeleteTable { name: "t".into() }] }).unwrap()).unwrap();
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
            sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("table-{v}") }] }).unwrap()).unwrap();
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
        sink.append(&FramedEntry::new(&WalEntry { version: 1, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap()).unwrap();
        sink.sync().unwrap();
        let after_first = std::fs::metadata(&path).unwrap().len();
        // A second small batch fits in the existing chunk: no physical growth.
        sink.append(&FramedEntry::new(&WalEntry { version: 2, ops: vec![WalOp::CreateTable { name: "u".into() }] }).unwrap()).unwrap();
        sink.sync().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), after_first);
    }

    #[test]
    fn prune_wal_prealloc_compacts_and_presizes() {
        let dir = crate::test_scratch::scratch_dir();
        let mut sink = PreallocFileSink::open_with_chunk(dir.path(), 4096).unwrap();
        for v in 1..=5u64 {
            sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] }).unwrap()).unwrap();
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
                sink.append(&FramedEntry::new(&WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] }).unwrap()).unwrap();
                sink.sync().unwrap();
            }
            sink.prune(2).unwrap();
            sink.append(&FramedEntry::new(&WalEntry { version: 5, ops: vec![WalOp::CreateTable { name: "t5".into() }] }).unwrap()).unwrap();
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
        sink.append(&FramedEntry::new(&WalEntry { version: 9, ops: vec![WalOp::CreateTable { name: "t".into() }] }).unwrap()).unwrap();
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

    /// The scan-tolerance policy is one decision, and these are its values.
    ///
    /// Both `PreallocFileSink::open_with_chunk` and `Store::recover` route
    /// through `tail_tolerant`, so they cannot disagree (#24) — a compile error
    /// covers a *new* sink. What is left to regress is a flipped arm, and both
    /// directions are bad: `CoalescedPrealloc => false` turns a benign torn
    /// tail into a hard error that costs the caller the whole durable log,
    /// while `FsWrite => true` silently truncates a genuinely corrupt one.
    #[test]
    fn scan_tolerance_is_one_decision_per_sink() {
        use crate::persistence::WalWrite;

        // Append-only sinks: a bad CRC is corruption and must be loud.
        assert!(!WalSinkKind::FsWrite.tail_tolerant());
        assert!(!WalSinkKind::Coalesced.tail_tolerant());
        assert!(!WalSinkKind::BufferedFile.tail_tolerant());
        // Presizing sink: a torn write into preallocated zeros is end-of-log.
        assert!(WalSinkKind::CoalescedPrealloc.tail_tolerant());

        // And the config surface maps onto exactly those sinks, so what
        // `Store::recover` computes for a given `WalWrite` is what the sink
        // for that same `WalWrite` applies.
        assert_eq!(WalWrite::PerEntry.sink_kind(), WalSinkKind::FsWrite);
        assert_eq!(WalWrite::Coalesced.sink_kind(), WalSinkKind::Coalesced);
        assert_eq!(WalWrite::CoalescedPrealloc.sink_kind(), WalSinkKind::CoalescedPrealloc);
        assert!(
            WalWrite::CoalescedPrealloc.sink_kind().tail_tolerant(),
            "the one production config that presizes must scan tolerantly"
        );
    }

    /// A failed extend must surface its own error and leave `capacity` where it
    /// was, so the retry re-extends from a size we know is durable.
    ///
    /// Scope, stated plainly: this is a **regression guard, not a proof of the
    /// rollback**. A read-only handle fails on the *first* write inside
    /// `preallocate_to`, so nothing is written and there is no partial
    /// extension to roll back — and both assertions below already held before
    /// the rollback existed, when the call was a bare `?`. What it pins is that
    /// a future edit to that error path cannot start swallowing the error or
    /// advancing `capacity`. Covering the rollback itself needs a write that
    /// succeeds and then fails partway (ENOSPC), which has no seam here.
    /// See issue #23.
    #[test]
    fn failed_extend_propagates_and_does_not_advance_capacity() {
        let dir = crate::test_scratch::scratch_dir();
        let path = dir.path().join(WAL_FILENAME);
        let mut f =
            OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        preallocate_to(&mut f, 0, 4096).unwrap();
        drop(f);

        // Read-only handle: the zero-fill cannot write, standing in for the
        // ENOSPC failure without needing to fill a disk.
        let ro = OpenOptions::new().read(true).open(&path).unwrap();
        let mut sink = PreallocFileSink {
            file: ro,
            path: path.clone(),
            buf: Vec::new(),
            write_head: 0,
            capacity: 4096,
            chunk: 4096,
        };
        // Enough bytes that `need > capacity` and the extend is attempted.
        sink.buf = vec![0xAB; 5000];

        assert!(sink.sync().is_err(), "the extend failure is surfaced, not swallowed");
        assert_eq!(sink.capacity, 4096, "capacity must not advance past a failed extend");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            4096,
            "on-disk size still the one that was sync_all'd"
        );
    }
}
