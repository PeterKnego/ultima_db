// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Checkpoint serialization and deserialization.
//!
//! A checkpoint is a full serialized snapshot of all tables at a specific version.
//! Used for fast recovery in both Standalone and SMR modes.
//!
//! File format (v2):
//! ```text
//! [magic: 4 bytes "ULDB"]
//! [format_version: u32]              // 2
//! [kind: u8]                         // CheckpointKind: Full=0, Delta=1
//! [snapshot_version: u64]
//! [base_version: u64]                // Delta only
//! [num_tables: u32]
//! for each table:
//!     [entry_kind: u8]               // TableEntryKind: Unchanged=0, Delta=1, Full=2, Dropped=3
//!     [name_len: u32][name: bytes]
//!     [data_len: u64][serialized table data: bytes]   // Full/Delta entries only
//! [crc32: u32]
//! ```
//!
//! Deltas deliberately share the `checkpoint_{version}.bin` filename namespace
//! with full checkpoints — see [`CheckpointKind`] for why.

#![allow(dead_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::registry::TableRegistry;
use crate::store::Snapshot;
use crate::wal::crc32;
use crate::{Error, Result};

const MAGIC: &[u8; 4] = b"ULDB";
const FORMAT_VERSION: u32 = 2;

/// What a `checkpoint_*.bin` file contains.
///
/// Deltas deliberately share the `checkpoint_{version}.bin` namespace with
/// full checkpoints. An older binary reading a delta-headed directory picks
/// the delta as "latest" and fails its format check loudly, instead of
/// silently loading an older full whose WAL tail has already been pruned —
/// which would lose committed data with no error anywhere.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum CheckpointKind {
    Full = 0,
    Delta = 1,
}

impl TryFrom<u8> for CheckpointKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CheckpointKind::Full),
            1 => Ok(CheckpointKind::Delta),
            other => Err(Error::CheckpointCorrupted(format!(
                "unknown checkpoint kind: {other}"
            ))),
        }
    }
}

/// How one table appears inside a checkpoint file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum TableEntryKind {
    /// Byte-identical to the base — no payload.
    Unchanged = 0,
    /// Changed rows only, as produced by `TableInfo::diff_table`.
    Delta = 1,
    /// Whole table inline, as produced by `TableInfo::serialize_table`.
    Full = 2,
    /// Present in the base, gone in this version — no payload.
    Dropped = 3,
}

impl TryFrom<u8> for TableEntryKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(TableEntryKind::Unchanged),
            1 => Ok(TableEntryKind::Delta),
            2 => Ok(TableEntryKind::Full),
            3 => Ok(TableEntryKind::Dropped),
            other => Err(Error::CheckpointCorrupted(format!(
                "unknown table entry kind: {other}"
            ))),
        }
    }
}

/// Serialize a snapshot to bytes using the type registry.
///
/// This always writes `CheckpointKind::Full` with every table entry as
/// `TableEntryKind::Full` — the delta path (task 6) is what will make this
/// function ever choose otherwise.
fn serialize_snapshot(snapshot: &Snapshot, registry: &TableRegistry) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    let mut buf = Vec::new();

    // Header
    buf.extend_from_slice(MAGIC);
    bincode::encode_into_std_write(FORMAT_VERSION, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(CheckpointKind::Full as u8, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(snapshot.version, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    // Only serialize tables that are registered in the registry.
    let registered_tables: Vec<(&String, &std::sync::Arc<dyn crate::table::MergeableTable>)> =
        snapshot
            .tables
            .iter()
            .filter(|(name, _)| registry.contains(name))
            .collect();

    bincode::encode_into_std_write(registered_tables.len() as u32, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    for (name, table_any) in registered_tables {
        let info = registry
            .get(name)
            .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;

        // Table entry kind — always Full in this task.
        bincode::encode_into_std_write(TableEntryKind::Full as u8, &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;

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
///
/// Only `CheckpointKind::Full` is understood in this task — `Delta` is
/// rejected with a named error rather than silently misparsed; task 6 gives
/// it real handling.
fn deserialize_snapshot(data: &[u8], registry: &TableRegistry) -> Result<Snapshot> {
    // Minimum: 4 (magic) + 1 (format_version varint, smallest encoding) + 4
    // (crc32) = 9 bytes. That's deliberately just enough to safely check the
    // magic and read a format version and bail on mismatch — not enough for
    // a full v2 header (kind/snapshot_version/num_tables). A v1 (or garbage)
    // file that's shorter than a real v2 header must still be rejected with
    // the *version* error, not "too short": that's the error message that
    // names what actually happened (see `a_v1_checkpoint_is_refused_with_a_named_version`).
    // Every field read past this point is itself bounds-checked, so a file
    // that passes this gate but is truncated later fails with a specific
    // error rather than a panic.
    if data.len() < 4 + 1 + 4 {
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

    // Checkpoint kind
    let (kind_byte, read): (u8, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    let kind = CheckpointKind::try_from(kind_byte)?;
    if kind != CheckpointKind::Full {
        // Delta bodies are produced starting task 6; this build never writes
        // them and doesn't yet know how to read them.
        return Err(Error::CheckpointCorrupted(format!(
            "checkpoint kind {kind:?} is not supported by this build"
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
        // Table entry kind
        let (entry_kind_byte, read): (u8, _) =
            bincode::decode_from_slice(&payload[offset..], config)
                .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;
        let entry_kind = TableEntryKind::try_from(entry_kind_byte)?;
        if entry_kind != TableEntryKind::Full {
            // Unchanged/Delta/Dropped entries are produced starting task 6;
            // this build only ever writes Full and doesn't yet know how to
            // read the others.
            return Err(Error::CheckpointCorrupted(format!(
                "table entry kind {entry_kind:?} is not supported by this build"
            )));
        }

        // Table name
        let (name, read): (String, _) = bincode::decode_from_slice(&payload[offset..], config)
            .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;

        // Table data length
        let (data_len, read): (u64, _) = bincode::decode_from_slice(&payload[offset..], config)
            .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;

        // Checked: the CRC only guards against accidental corruption, so a
        // crafted/garbled length must not overflow into a panic.
        let end = usize::try_from(data_len)
            .ok()
            .and_then(|l| offset.checked_add(l))
            .ok_or_else(|| Error::CheckpointCorrupted("table data length overflow".into()))?;
        if end > payload.len() {
            return Err(Error::CheckpointCorrupted("truncated table data".into()));
        }
        let table_bytes = &payload[offset..end];
        offset = end;

        let info = registry
            .get(&name)
            .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;
        // Name the table in format/corruption errors: `deserialize_table` sees
        // only an anonymous byte slice, and the message an operator hits after
        // upgrading with existing data on disk ("unsupported table format
        // version") is useless without knowing *which* table it came from.
        // Only `Persistence` — the variant this file's format errors use — is
        // rewritten; a `UniqueConstraintViolation` from the index rebuild
        // keeps its own variant so callers can still match on it.
        let table_any = (info.deserialize_table)(table_bytes).map_err(|e| match e {
            Error::Persistence(msg) => Error::Persistence(format!("table '{name}': {msg}")),
            other => other,
        })?;
        tables.insert(name, std::sync::Arc::from(table_any));
    }

    Ok(Snapshot { version, tables })
}

/// Serialize `snapshot` as a delta against `base`: unchanged tables cost a
/// single byte-and-a-name, changed tables carry only their changed rows, and
/// created/dropped/type-changed tables are called out by an explicit entry
/// kind rather than left for the loader to infer.
///
/// Entries are decided into a `Vec` before anything is written so
/// `num_tables` can be a true count instead of a reserved-and-backfilled
/// length — the latter silently desynchronises the moment an entry kind is
/// added later and one write site forgets to update the placeholder.
fn serialize_delta(
    snapshot: &Snapshot,
    base: &Snapshot,
    registry: &TableRegistry,
) -> Result<Vec<u8>> {
    struct Entry<'a> {
        name: &'a str,
        kind: TableEntryKind,
        payload: Option<Vec<u8>>,
    }

    let mut entries: Vec<Entry> = Vec::new();

    for (name, table) in snapshot
        .tables
        .iter()
        .filter(|(name, _)| registry.contains(name))
    {
        let info = registry
            .get(name)
            .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;
        match base.tables.get(name) {
            // Same Arc: the table was not touched since the base checkpoint,
            // so there is provably nothing to write.
            Some(base_table) if std::sync::Arc::ptr_eq(table, base_table) => {
                entries.push(Entry {
                    name,
                    kind: TableEntryKind::Unchanged,
                    payload: None,
                });
            }
            Some(base_table) => {
                match (info.diff_table)(table.as_ref().as_any(), base_table.as_ref().as_any()) {
                    Ok(payload) => entries.push(Entry {
                        name,
                        kind: TableEntryKind::Delta,
                        payload: Some(payload),
                    }),
                    // Dropped and recreated with a different R or K: the base
                    // rows are not comparable, so the whole table goes inline.
                    Err(Error::TableTypeChanged { .. }) => {
                        let payload = (info.serialize_table)(table.as_ref().as_any())?;
                        entries.push(Entry {
                            name,
                            kind: TableEntryKind::Full,
                            payload: Some(payload),
                        });
                    }
                    Err(e) => return Err(e),
                }
            }
            None => {
                let payload = (info.serialize_table)(table.as_ref().as_any())?;
                entries.push(Entry {
                    name,
                    kind: TableEntryKind::Full,
                    payload: Some(payload),
                });
            }
        }
    }

    // Tables in the base that are gone from this snapshot.
    for name in base.tables.keys() {
        if !snapshot.tables.contains_key(name) && registry.contains(name) {
            entries.push(Entry {
                name,
                kind: TableEntryKind::Dropped,
                payload: None,
            });
        }
    }

    let config = bincode::config::standard();
    let mut buf = Vec::new();

    // Header
    buf.extend_from_slice(MAGIC);
    bincode::encode_into_std_write(FORMAT_VERSION, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(CheckpointKind::Delta as u8, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(snapshot.version, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(base.version, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    bincode::encode_into_std_write(entries.len() as u32, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    for entry in &entries {
        bincode::encode_into_std_write(entry.kind as u8, &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        bincode::encode_into_std_write(entry.name, &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        // Unchanged/Dropped entries carry no payload at all — not even a
        // zero-length one — so the loader's byte accounting matches what was
        // actually decided above, not a placeholder for something that was
        // never computed.
        if let Some(payload) = &entry.payload {
            bincode::encode_into_std_write(payload.len() as u64, &mut buf, config)
                .map_err(|e| Error::Persistence(e.to_string()))?;
            buf.extend_from_slice(payload);
        }
    }

    // Append CRC32 of everything before it — the delta payload itself
    // carries no CRC or length trailer of its own (it mirrors
    // `serialize_table`'s framing), so this whole-file checksum is what
    // protects it.
    let checksum = crc32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    Ok(buf)
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

/// Write already-serialized checkpoint bytes to `checkpoint_{version}.bin`,
/// via write-to-temp + `sync_all` + atomic rename + `sync_dir` — the crash
/// safety dance shared by both the full and delta writers, so a process
/// crash mid-write never leaves a corrupt or partially-visible checkpoint
/// file.
fn write_checkpoint_bytes(dir: &Path, version: u64, data: &[u8]) -> Result<u64> {
    std::fs::create_dir_all(dir).map_err(|e| Error::Persistence(e.to_string()))?;

    let final_path = dir.join(checkpoint_filename(version));
    let tmp_path = dir.join(format!("{}.tmp", checkpoint_filename(version)));

    let mut file = File::create(&tmp_path).map_err(|e| Error::Persistence(e.to_string()))?;
    file.write_all(data)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    file.sync_all()
        .map_err(|e| Error::Persistence(e.to_string()))?;
    drop(file);
    std::fs::rename(&tmp_path, &final_path).map_err(|e| Error::Persistence(e.to_string()))?;
    crate::wal::sync_dir(dir)?;

    Ok(version)
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
    let data = serialize_snapshot(snapshot, registry)?;
    write_checkpoint_bytes(dir, snapshot.version, &data)
}

/// Write a delta checkpoint — `snapshot` serialized against `base`, so
/// tables untouched since `base` cost no payload and only genuinely changed
/// rows are written. Shares the `checkpoint_{version}.bin` namespace and
/// crash-safety discipline with [`write_checkpoint`]; see [`CheckpointKind`]
/// for why deltas are not given their own filename pattern.
pub(crate) fn write_delta_checkpoint(
    dir: &Path,
    snapshot: &Snapshot,
    base: &Snapshot,
    registry: &TableRegistry,
) -> Result<u64> {
    let data = serialize_delta(snapshot, base, registry)?;
    write_checkpoint_bytes(dir, snapshot.version, &data)
}

/// Load a checkpoint from a file.
pub(crate) fn load_checkpoint(path: &Path, registry: &TableRegistry) -> Result<Snapshot> {
    let mut file = File::open(path).map_err(|e| Error::Persistence(e.to_string()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| Error::Persistence(e.to_string()))?;
    deserialize_snapshot(&data, registry)
}

/// Bytes needed to decode a checkpoint header: magic (4) + `format_version`
/// varint (bincode caps a u32 varint at 5 bytes) + `kind` (1) +
/// `snapshot_version` varint (u64 varint, capped at 9 bytes) + `base_version`
/// varint (9, `Delta` only). Reading this bounded prefix — instead of the
/// whole file — is what makes a chain walk cost O(chain length) in bytes
/// read, not O(sum of every checkpoint's full size): a chain can have
/// arbitrarily many full-table-sized deltas behind the head.
const HEADER_PREFIX_LEN: usize = 4 + 5 + 1 + 9 + 9;

/// Read just enough of `path` to learn its `CheckpointKind`, snapshot
/// version, and (for a `Delta`) base version — without reading the table
/// entries that make up the rest of the file.
fn read_header(path: &Path) -> Result<(CheckpointKind, u64, Option<u64>)> {
    let mut file = File::open(path).map_err(|e| Error::Persistence(e.to_string()))?;
    let mut buf = Vec::with_capacity(HEADER_PREFIX_LEN);
    (&mut file)
        .take(HEADER_PREFIX_LEN as u64)
        .read_to_end(&mut buf)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    if buf.len() < 4 || &buf[0..4] != MAGIC {
        return Err(Error::CheckpointCorrupted("bad magic".into()));
    }
    let config = bincode::config::standard();
    let mut offset = 4;

    let (fmt_version, read): (u32, _) = bincode::decode_from_slice(&buf[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    if fmt_version != FORMAT_VERSION {
        return Err(Error::CheckpointCorrupted(format!(
            "unsupported format version: {fmt_version}"
        )));
    }

    let (kind_byte, read): (u8, _) = bincode::decode_from_slice(&buf[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    let kind = CheckpointKind::try_from(kind_byte)?;

    let (version, read): (u64, _) = bincode::decode_from_slice(&buf[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;

    let base_version = if kind == CheckpointKind::Delta {
        let (bv, _read): (u64, _) = bincode::decode_from_slice(&buf[offset..], config)
            .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        Some(bv)
    } else {
        None
    };

    Ok((kind, version, base_version))
}

/// Walk backwards from the highest-versioned `checkpoint_*.bin` file in
/// `dir` to the nearest `Full` ancestor, returning the chain base-first (so
/// [`load_chain`] can apply it in order). Empty if no checkpoint exists.
///
/// Only headers are read (see [`read_header`]) — the chain walk never reads
/// a table entry.
///
/// A file that is simply absent (deleted by `cleanup_old_checkpoints`, or
/// never written) is expected once the walk passes the newest surviving
/// file: tmp+rename makes a checkpoint atomically present or absent, so an
/// absent file below the walk's starting point just means the head *is*
/// that file, a complete chain on its own. What must never happen is an
/// absent file *in the middle* of a chain the walk has already committed to
/// by reading a `Delta` header that names it as `base_version` — that is
/// [`Error::CheckpointChainBroken`], and it is not recoverable by falling
/// back to an older `Full`: the WAL is already pruned to the head version,
/// so an older full plus the surviving WAL reconstructs less than was
/// committed, silently.
pub(crate) fn find_head_chain(dir: &Path) -> Result<Vec<PathBuf>> {
    let head_path = match find_latest_checkpoint(dir)? {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };

    let mut chain = vec![head_path.clone()];
    let mut current_path = head_path;
    let mut head_version: Option<u64> = None;

    loop {
        let (kind, version, base_version) = read_header(&current_path)?;
        let head_version = *head_version.get_or_insert(version);

        match kind {
            CheckpointKind::Full => break,
            CheckpointKind::Delta => {
                // Written by `serialize_delta`, which always emits a
                // `base_version` for `CheckpointKind::Delta` — `None` here
                // would mean `read_header`'s own encoding disagrees with the
                // writer's, not a possibility a corrupt file on disk can
                // trigger (a corrupt `has_next_id`-style byte fails the
                // decode above instead).
                let base_version = base_version.expect(
                    "read_header always returns Some(base_version) for CheckpointKind::Delta",
                );
                let base_path = dir.join(checkpoint_filename(base_version));
                if !base_path.exists() {
                    return Err(Error::CheckpointChainBroken {
                        head: head_version,
                        missing: base_version,
                    });
                }
                chain.push(base_path.clone());
                current_path = base_path;
            }
        }
    }

    chain.reverse();
    Ok(chain)
}

/// Apply one delta file onto `base`, producing the snapshot at the delta's
/// own version. `base` must be the snapshot at the delta's recorded
/// `base_version` — [`load_chain`] enforces this by construction (it walks
/// `paths` in the order [`find_head_chain`] returned), and this function
/// double-checks it against the file's own header rather than trusting the
/// caller silently.
fn apply_delta_file(path: &Path, base: Snapshot, registry: &TableRegistry) -> Result<Snapshot> {
    let mut file = File::open(path).map_err(|e| Error::Persistence(e.to_string()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    if data.len() < 4 + 1 + 4 {
        return Err(Error::CheckpointCorrupted("file too short".into()));
    }

    // Whole-file CRC first — a delta's per-table entries carry no CRC of
    // their own (see the module doc comment), so this is what protects them.
    let crc_offset = data.len() - 4;
    let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
    let computed_crc = crc32(&data[..crc_offset]);
    if stored_crc != computed_crc {
        return Err(Error::CheckpointCorrupted("CRC mismatch".into()));
    }

    let payload = &data[..crc_offset];
    let config = bincode::config::standard();
    let mut offset = 0;

    if &payload[offset..offset + 4] != MAGIC {
        return Err(Error::CheckpointCorrupted("bad magic".into()));
    }
    offset += 4;

    let (fmt_version, read): (u32, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    if fmt_version != FORMAT_VERSION {
        return Err(Error::CheckpointCorrupted(format!(
            "unsupported format version: {fmt_version}"
        )));
    }

    let (kind_byte, read): (u8, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    let kind = CheckpointKind::try_from(kind_byte)?;
    if kind != CheckpointKind::Delta {
        return Err(Error::CheckpointCorrupted(format!(
            "expected a delta checkpoint at {}, found {kind:?}",
            path.display()
        )));
    }

    let (version, read): (u64, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;

    let (base_version, read): (u64, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;
    if base_version != base.version {
        return Err(Error::CheckpointCorrupted(format!(
            "delta at {} expects base version {base_version}, but the chain so far is at {}",
            path.display(),
            base.version
        )));
    }

    let (num_tables, read): (u32, _) = bincode::decode_from_slice(&payload[offset..], config)
        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
    offset += read;

    let mut tables = base.tables;

    for _ in 0..num_tables {
        let (entry_kind_byte, read): (u8, _) =
            bincode::decode_from_slice(&payload[offset..], config)
                .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;
        let entry_kind = TableEntryKind::try_from(entry_kind_byte)?;

        let (name, read): (String, _) = bincode::decode_from_slice(&payload[offset..], config)
            .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
        offset += read;

        match entry_kind {
            // Byte-identical to what the accumulated table already holds —
            // nothing to read, nothing to change.
            TableEntryKind::Unchanged => {}
            // Present in the base, gone as of this version. Every drop is
            // explicit (see `serialize_delta`'s doc comment / task brief
            // contract 3), so there is no other place in this loop that
            // needs to infer a removal from an entry's absence.
            TableEntryKind::Dropped => {
                tables.remove(&name);
            }
            TableEntryKind::Full | TableEntryKind::Delta => {
                let (data_len, read): (u64, _) =
                    bincode::decode_from_slice(&payload[offset..], config)
                        .map_err(|e| Error::CheckpointCorrupted(e.to_string()))?;
                offset += read;
                let end = usize::try_from(data_len)
                    .ok()
                    .and_then(|l| offset.checked_add(l))
                    .ok_or_else(|| {
                        Error::CheckpointCorrupted("table data length overflow".into())
                    })?;
                if end > payload.len() {
                    return Err(Error::CheckpointCorrupted("truncated table data".into()));
                }
                let table_bytes = &payload[offset..end];
                offset = end;

                let info = registry
                    .get(&name)
                    .ok_or_else(|| Error::TableNotRegistered(name.clone()))?;

                if entry_kind == TableEntryKind::Full {
                    let table_any = (info.deserialize_table)(table_bytes).map_err(|e| match e {
                        Error::Persistence(msg) => {
                            Error::Persistence(format!("table '{name}': {msg}"))
                        }
                        other => other,
                    })?;
                    tables.insert(name, std::sync::Arc::from(table_any));
                } else {
                    // A `Delta` entry is only ever emitted for a table
                    // `serialize_delta` found in *both* base and new (see its
                    // `Some(base_table)` match arm) — by the time a chain
                    // reaches here the accumulator mirrors that same base, so
                    // this table must already be present. Its absence means
                    // corruption, not a legal chain state.
                    let existing = tables.get(&name).ok_or_else(|| {
                        Error::CheckpointCorrupted(format!(
                            "delta entry for table '{name}' has no base table to apply onto"
                        ))
                    })?;
                    let mut boxed = existing.boxed_clone();
                    crate::registry::apply_delta(boxed.as_any_mut(), table_bytes, info).map_err(
                        |e| match e {
                            Error::Persistence(msg) => {
                                Error::Persistence(format!("table '{name}': {msg}"))
                            }
                            other => other,
                        },
                    )?;
                    tables.insert(name, std::sync::Arc::from(boxed));
                }
            }
        }
    }

    if offset != payload.len() {
        return Err(Error::CheckpointCorrupted(format!(
            "table entries did not exactly consume the checkpoint payload: {} trailing bytes",
            payload.len() - offset
        )));
    }

    Ok(Snapshot { version, tables })
}

/// Load a checkpoint chain — a base `Full` file plus zero or more `Delta`
/// files, in the order [`find_head_chain`] returns — into the `Snapshot` at
/// the chain's head version.
///
/// The base is loaded through the existing full-checkpoint path
/// ([`load_checkpoint`]/`deserialize_snapshot`), which already refuses
/// anything but `CheckpointKind::Full`. Each subsequent delta is folded onto
/// the accumulated result by [`apply_delta_file`].
pub(crate) fn load_chain(paths: &[PathBuf], registry: &TableRegistry) -> Result<Snapshot> {
    let (base_path, deltas) = paths
        .split_first()
        .ok_or_else(|| Error::CheckpointCorrupted("empty checkpoint chain".into()))?;

    let mut snapshot = load_checkpoint(base_path, registry)?;
    for delta_path in deltas {
        snapshot = apply_delta_file(delta_path, snapshot, registry)?;
    }
    Ok(snapshot)
}

/// Delete checkpoint files *older* than `keep_version`.
///
/// Newer checkpoints are never deleted: a slower checkpoint finishing after
/// a faster concurrent one must not remove the newer file — it may be the
/// only checkpoint covering WAL entries the faster checkpoint already
/// pruned, and deleting it would make those commits unrecoverable.
/// Unparseable `checkpoint_*.bin` names are left alone.
pub(crate) fn cleanup_old_checkpoints(dir: &Path, keep_version: u64) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    for entry in entries {
        let entry = entry.map_err(|e| Error::Persistence(e.to_string()))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(version) = name_str
            .strip_prefix("checkpoint_")
            .and_then(|s| s.strip_suffix(".bin"))
            .and_then(|s| s.parse::<u64>().ok())
            && version < keep_version
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

    /// A second, unrelated record type — stands in for what a table looked
    /// like *before* it was dropped and recreated under the same name, in
    /// `a_table_recreated_with_a_different_type_is_recorded_in_full` below.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Widget {
        label: String,
    }

    fn make_snapshot_with_users() -> (Snapshot, TableRegistry) {
        let mut reg = TableRegistry::default();
        reg.register::<User, u64>("users").unwrap();

        let mut table = Table::<User>::new();
        table
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        table
            .insert(User {
                name: "Bob".into(),
                age: 25,
            })
            .unwrap();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert(
            "users".to_string(),
            std::sync::Arc::new(table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );

        let snapshot = Snapshot {
            version: 42,
            tables,
        };
        (snapshot, reg)
    }

    #[test]
    fn v2_full_checkpoint_round_trips() {
        let (snap, registry) = make_snapshot_with_users();
        let bytes = serialize_snapshot(&snap, &registry).unwrap();
        assert_eq!(&bytes[0..4], MAGIC);
        let restored = deserialize_snapshot(&bytes, &registry).unwrap();
        assert_eq!(restored.version, snap.version);
        assert_eq!(restored.tables.len(), snap.tables.len());
    }

    #[test]
    fn a_v1_checkpoint_is_refused_with_a_named_version() {
        let mut v1 = Vec::new();
        v1.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(1u32, &mut v1, bincode::config::standard()).unwrap();
        v1.extend_from_slice(&crc32(&v1).to_le_bytes());
        let registry = TableRegistry::default();
        let Err(err) = deserialize_snapshot(&v1, &registry) else {
            panic!("a v1 checkpoint must be rejected");
        };
        assert!(
            matches!(err, Error::CheckpointCorrupted(ref m) if m.contains("unsupported format version: 1")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn checkpoint_serialize_deserialize_roundtrip() {
        let (snapshot, reg) = make_snapshot_with_users();
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();
        assert_eq!(recovered.version, 42);
        let table = recovered
            .tables
            .get("users")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<User>>()
            .unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(
            table.get(&1).unwrap(),
            &User {
                name: "Alice".into(),
                age: 30
            }
        );
        assert_eq!(
            table.get(&2).unwrap(),
            &User {
                name: "Bob".into(),
                age: 25
            }
        );
    }

    #[test]
    fn checkpoint_crc_corruption_detected() {
        let (snapshot, reg) = make_snapshot_with_users();
        let mut data = serialize_snapshot(&snapshot, &reg).unwrap();
        data[10] ^= 0xFF; // corrupt a byte
        assert!(matches!(
            deserialize_snapshot(&data, &reg),
            Err(Error::CheckpointCorrupted(_))
        ));
    }

    #[test]
    fn checkpoint_file_write_and_load() {
        let dir = crate::test_scratch::scratch_dir();
        let (snapshot, reg) = make_snapshot_with_users();

        write_checkpoint(dir.path(), &snapshot, &reg).unwrap();
        let path = find_latest_checkpoint(dir.path()).unwrap().unwrap();
        let recovered = load_checkpoint(&path, &reg).unwrap();
        assert_eq!(recovered.version, 42);
    }

    #[test]
    fn find_latest_checkpoint_picks_highest_version() {
        let dir = crate::test_scratch::scratch_dir();
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
        let dir = crate::test_scratch::scratch_dir();
        let (snapshot, reg) = make_snapshot_with_users();

        let mut snap10 = snapshot.clone();
        snap10.version = 10;
        write_checkpoint(dir.path(), &snap10, &reg).unwrap();
        write_checkpoint(dir.path(), &snapshot, &reg).unwrap(); // version 42

        cleanup_old_checkpoints(dir.path(), 42).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].contains("checkpoint_42"));
    }

    /// A slow checkpoint at version V must never delete a checkpoint a
    /// faster concurrent checkpoint wrote at a *newer* version — that
    /// newer checkpoint may be the only one covering already-pruned WAL
    /// entries; deleting it makes those commits unrecoverable.
    #[test]
    fn cleanup_old_checkpoints_never_deletes_newer() {
        let dir = crate::test_scratch::scratch_dir();
        let (snapshot, reg) = make_snapshot_with_users();

        for v in [8u64, 10, 12] {
            let mut snap = snapshot.clone();
            snap.version = v;
            write_checkpoint(dir.path(), &snap, &reg).unwrap();
        }

        cleanup_old_checkpoints(dir.path(), 10).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            !files.iter().any(|f| f.contains("checkpoint_8")),
            "older checkpoint should be removed: {files:?}"
        );
        assert!(
            files.iter().any(|f| f.contains("checkpoint_10")),
            "kept checkpoint missing: {files:?}"
        );
        assert!(
            files.iter().any(|f| f.contains("checkpoint_12")),
            "newer checkpoint must never be deleted: {files:?}"
        );
    }

    /// A crafted checkpoint with a *valid* whole-file CRC but an absurd
    /// `data_len` must produce `CheckpointCorrupted`, not an arithmetic
    /// overflow panic (debug) or wrapped-slice panic (release). The CRC only
    /// guards against accidental corruption; length fields still need
    /// checked arithmetic.
    #[test]
    fn deserialize_huge_table_len_errors_not_panics() {
        let config = bincode::config::standard();
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(FORMAT_VERSION, &mut buf, config).unwrap();
        bincode::encode_into_std_write(CheckpointKind::Full as u8, &mut buf, config).unwrap();
        bincode::encode_into_std_write(7u64, &mut buf, config).unwrap(); // snapshot version
        bincode::encode_into_std_write(1u32, &mut buf, config).unwrap(); // num_tables
        bincode::encode_into_std_write(TableEntryKind::Full as u8, &mut buf, config).unwrap();
        bincode::encode_into_std_write("users", &mut buf, config).unwrap();
        bincode::encode_into_std_write(u64::MAX, &mut buf, config).unwrap(); // data_len
        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let reg = TableRegistry::default();
        match deserialize_snapshot(&buf, &reg) {
            Err(Error::CheckpointCorrupted(_)) => {}
            Err(other) => panic!("expected CheckpointCorrupted, got {other:?}"),
            Ok(_) => panic!("expected CheckpointCorrupted, got Ok"),
        }
    }

    #[test]
    fn deserialize_too_short_errors() {
        let reg = TableRegistry::default();
        let data = vec![0u8; 5]; // too short for any valid checkpoint
        let result = deserialize_snapshot(&data, &reg);
        assert!(
            matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("too short"))
        );
    }

    #[test]
    fn deserialize_bad_magic_errors() {
        let config = bincode::config::standard();
        let reg = TableRegistry::default();
        // Build a payload with wrong magic but valid structure and CRC
        let mut data = Vec::new();
        data.extend_from_slice(b"XXXX"); // bad magic
        bincode::encode_into_std_write(FORMAT_VERSION, &mut data, config).unwrap();
        bincode::encode_into_std_write(CheckpointKind::Full as u8, &mut data, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap();
        bincode::encode_into_std_write(0u32, &mut data, config).unwrap();
        let checksum = crc32(&data);
        data.extend_from_slice(&checksum.to_le_bytes());
        let result = deserialize_snapshot(&data, &reg);
        assert!(
            matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("bad magic"))
        );
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
        bincode::encode_into_std_write(CheckpointKind::Full as u8, &mut data, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // version
        bincode::encode_into_std_write(1u32, &mut data, config).unwrap(); // 1 table

        // Table entry kind
        bincode::encode_into_std_write(TableEntryKind::Full as u8, &mut data, config).unwrap();
        // Table name
        bincode::encode_into_std_write("users", &mut data, config).unwrap();
        // Claim data_len = 9999 but don't write that much data
        bincode::encode_into_std_write(9999u64, &mut data, config).unwrap();

        let checksum = crc32(&data);
        data.extend_from_slice(&checksum.to_le_bytes());

        let mut reg = TableRegistry::default();
        reg.register::<User, u64>("users").unwrap();
        let result = deserialize_snapshot(&data, &reg);
        assert!(
            matches!(result, Err(Error::CheckpointCorrupted(ref msg)) if msg.contains("truncated"))
        );
    }

    /// The upgrade an operator actually performs: a checkpoint written by a
    /// pre-0.3.0 build, whose table bodies are in table format v1. It must be
    /// rejected, and the error must name *which* table — `deserialize_table`
    /// sees only an anonymous byte slice.
    #[test]
    fn deserialize_v1_table_body_errors_and_names_the_table() {
        let config = bincode::config::standard();

        // A real v1 body: [next_id][count][id, record]*, all bincode varint.
        let mut body = Vec::new();
        bincode::encode_into_std_write(2u64, &mut body, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut body, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut body, config).unwrap();
        bincode::serde::encode_into_std_write(
            &User {
                name: "Alice".into(),
                age: 30,
            },
            &mut body,
            config,
        )
        .unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        bincode::encode_into_std_write(FORMAT_VERSION, &mut data, config).unwrap();
        bincode::encode_into_std_write(CheckpointKind::Full as u8, &mut data, config).unwrap();
        bincode::encode_into_std_write(1u64, &mut data, config).unwrap(); // snapshot version
        bincode::encode_into_std_write(1u32, &mut data, config).unwrap(); // num_tables
        bincode::encode_into_std_write(TableEntryKind::Full as u8, &mut data, config).unwrap();
        bincode::encode_into_std_write("users", &mut data, config).unwrap();
        bincode::encode_into_std_write(body.len() as u64, &mut data, config).unwrap();
        data.extend_from_slice(&body);
        let crc = crc32(&data);
        data.extend_from_slice(&crc.to_le_bytes());

        let mut reg = TableRegistry::default();
        reg.register::<User, u64>("users").unwrap();
        let Err(err) = deserialize_snapshot(&data, &reg) else {
            panic!("a v1 table body must be rejected");
        };
        let msg = format!("{err}");
        assert!(msg.contains("format version"), "{msg}");
        assert!(msg.contains("users"), "the error must name the table: {msg}");
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
        let result = find_latest_checkpoint(std::path::Path::new(
            "/nonexistent/path/that/does/not/exist",
        ))
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_latest_checkpoint_empty_dir() {
        let dir = crate::test_scratch::scratch_dir();
        let result = find_latest_checkpoint(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_latest_checkpoint_ignores_non_checkpoint_files() {
        let dir = crate::test_scratch::scratch_dir();
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
        let snapshot = Snapshot {
            version: 1,
            tables: std::collections::BTreeMap::new(),
        };
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();
        assert_eq!(recovered.version, 1);
        assert!(recovered.tables.is_empty());
    }

    #[test]
    fn multi_table_snapshot_roundtrip() {
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Order {
            item: String,
            qty: u32,
        }

        let mut reg = TableRegistry::default();
        reg.register::<User, u64>("users").unwrap();
        reg.register::<Order, u64>("orders").unwrap();

        let mut user_table = Table::<User>::new();
        user_table
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();

        let mut order_table = Table::<Order>::new();
        order_table
            .insert(Order {
                item: "Widget".into(),
                qty: 5,
            })
            .unwrap();
        order_table
            .insert(Order {
                item: "Gadget".into(),
                qty: 3,
            })
            .unwrap();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert(
            "users".to_string(),
            std::sync::Arc::new(user_table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        tables.insert(
            "orders".to_string(),
            std::sync::Arc::new(order_table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );

        let snapshot = Snapshot { version: 7, tables };
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();

        assert_eq!(recovered.version, 7);
        assert_eq!(recovered.tables.len(), 2);

        let users = recovered
            .tables
            .get("users")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<User>>()
            .unwrap();
        assert_eq!(users.len(), 1);

        let orders = recovered
            .tables
            .get("orders")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<Order>>()
            .unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders.get(&1).unwrap().item, "Widget");
        assert_eq!(orders.get(&2).unwrap().qty, 3);
    }

    #[test]
    fn snapshot_with_unregistered_table_skips_it() {
        // Snapshot has "users" and "logs", but only "users" is registered
        let mut reg = TableRegistry::default();
        reg.register::<User, u64>("users").unwrap();

        let mut user_table = Table::<User>::new();
        user_table
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();

        let log_table = Table::<String>::new();

        let mut tables = std::collections::BTreeMap::new();
        tables.insert(
            "users".to_string(),
            std::sync::Arc::new(user_table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        tables.insert(
            "logs".to_string(),
            std::sync::Arc::new(log_table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );

        let snapshot = Snapshot { version: 1, tables };
        let data = serialize_snapshot(&snapshot, &reg).unwrap();
        let recovered = deserialize_snapshot(&data, &reg).unwrap();

        // Only "users" should be in the recovered snapshot
        assert_eq!(recovered.tables.len(), 1);
        assert!(recovered.tables.contains_key("users"));
    }

    #[test]
    fn write_checkpoint_creates_tmp_then_renames() {
        let dir = crate::test_scratch::scratch_dir();
        let (snapshot, reg) = make_snapshot_with_users();

        write_checkpoint(dir.path(), &snapshot, &reg).unwrap();

        // Final file should exist, tmp should not
        let final_path = dir.path().join("checkpoint_42.bin");
        let tmp_path = dir.path().join("checkpoint_42.bin.tmp");
        assert!(final_path.exists());
        assert!(!tmp_path.exists());
    }

    /// One decoded table entry: its kind, and its raw payload bytes for
    /// `Full`/`Delta` (`None` for `Unchanged`/`Dropped`, which carry none).
    struct ParsedEntry {
        kind: TableEntryKind,
        payload: Option<Vec<u8>>,
    }

    /// Parse a delta (or full) checkpoint file's header and every per-table
    /// entry — verifying the whole-file CRC and that the entries exactly
    /// consume the payload with no trailing or overrun bytes. That
    /// exactly-self-delimiting property is precisely what Task 6's loader
    /// will depend on to find the next entry, so tests built on this parser
    /// prove it on every call rather than assuming it.
    fn parse_checkpoint_entries(raw: &[u8]) -> std::collections::HashMap<String, ParsedEntry> {
        let config = bincode::config::standard();
        let crc_offset = raw.len() - 4;
        let payload = &raw[..crc_offset];
        let stored_crc = u32::from_le_bytes(raw[crc_offset..].try_into().unwrap());
        assert_eq!(crc32(payload), stored_crc, "checkpoint file CRC mismatch");
        let mut offset = 4; // magic

        let (_fmt_version, read): (u32, _) =
            bincode::decode_from_slice(&payload[offset..], config).unwrap();
        offset += read;

        let (kind_byte, read): (u8, _) =
            bincode::decode_from_slice(&payload[offset..], config).unwrap();
        offset += read;
        let kind = CheckpointKind::try_from(kind_byte).unwrap();

        let (_snapshot_version, read): (u64, _) =
            bincode::decode_from_slice(&payload[offset..], config).unwrap();
        offset += read;

        if kind == CheckpointKind::Delta {
            let (_base_version, read): (u64, _) =
                bincode::decode_from_slice(&payload[offset..], config).unwrap();
            offset += read;
        }

        let (num_tables, read): (u32, _) =
            bincode::decode_from_slice(&payload[offset..], config).unwrap();
        offset += read;

        let mut out = std::collections::HashMap::new();
        for _ in 0..num_tables {
            let (entry_kind_byte, read): (u8, _) =
                bincode::decode_from_slice(&payload[offset..], config).unwrap();
            offset += read;
            let entry_kind = TableEntryKind::try_from(entry_kind_byte).unwrap();

            let (name, read): (String, _) =
                bincode::decode_from_slice(&payload[offset..], config).unwrap();
            offset += read;

            let is_inline = matches!(entry_kind, TableEntryKind::Full | TableEntryKind::Delta);
            let table_payload = if is_inline {
                let (data_len, read): (u64, _) =
                    bincode::decode_from_slice(&payload[offset..], config).unwrap();
                offset += read;
                let end = offset + data_len as usize;
                let bytes = payload[offset..end].to_vec();
                offset = end;
                Some(bytes)
            } else {
                None
            };

            out.insert(
                name,
                ParsedEntry {
                    kind: entry_kind,
                    payload: table_payload,
                },
            );
        }

        // Entries must exactly consume the payload: no unparsed trailing
        // bytes (a length that undershot) and no overrun into the CRC (a
        // length that overshot — `payload[offset..end]` above would already
        // have panicked, but this also catches a *short* final entry).
        assert_eq!(
            offset,
            payload.len(),
            "table entries did not exactly consume the checkpoint payload"
        );

        out
    }

    /// [`parse_checkpoint_entries`], keeping only the entry kind — enough
    /// for tests that only care which tables landed as
    /// `Unchanged`/`Delta`/`Full`/`Dropped`.
    fn parse_table_entry_kinds(raw: &[u8]) -> std::collections::HashMap<String, TableEntryKind> {
        parse_checkpoint_entries(raw)
            .into_iter()
            .map(|(name, entry)| (name, entry.kind))
            .collect()
    }

    /// Build two snapshots sharing table Arcs except where the test wants a
    /// difference: `base` has "a" and "b"; `new` is `base` with "a" replaced
    /// by a table with an extra row, so "b"'s `Arc` is byte-identical between
    /// the two snapshots.
    fn make_base_and_changed_a() -> (Snapshot, Snapshot, TableRegistry) {
        let mut reg = TableRegistry::default();
        reg.register::<User, u64>("a").unwrap();
        reg.register::<User, u64>("b").unwrap();

        let mut table_a = Table::<User>::new();
        table_a
            .insert(User {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();

        let mut table_b = Table::<User>::new();
        table_b
            .insert(User {
                name: "Bob".into(),
                age: 25,
            })
            .unwrap();
        let table_b: std::sync::Arc<dyn crate::table::MergeableTable> =
            std::sync::Arc::new(table_b);

        let mut base_tables = std::collections::BTreeMap::new();
        base_tables.insert(
            "a".to_string(),
            std::sync::Arc::new(table_a.clone()) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        base_tables.insert("b".to_string(), table_b.clone());
        let base_snap = Snapshot {
            version: 1,
            tables: base_tables,
        };

        table_a
            .insert(User {
                name: "Carol".into(),
                age: 40,
            })
            .unwrap();
        let mut new_tables = std::collections::BTreeMap::new();
        new_tables.insert(
            "a".to_string(),
            std::sync::Arc::new(table_a) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        // Same Arc as base — "b" was not touched in this interval.
        new_tables.insert("b".to_string(), table_b);
        let new_snap = Snapshot {
            version: 2,
            tables: new_tables,
        };

        (base_snap, new_snap, reg)
    }

    #[test]
    fn a_delta_records_only_changed_tables() {
        let dir = crate::test_scratch::scratch_dir();
        let (base_snap, new_snap, registry) = make_base_and_changed_a();

        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap();
        assert_eq!(v, new_snap.version);

        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        let entries = parse_table_entry_kinds(&raw);
        assert_eq!(entries.get("a"), Some(&TableEntryKind::Delta));
        assert_eq!(entries.get("b"), Some(&TableEntryKind::Unchanged));
    }

    #[test]
    fn a_table_absent_from_the_new_snapshot_is_recorded_as_dropped() {
        let dir = crate::test_scratch::scratch_dir();
        let (base_snap, mut new_snap, registry) = make_base_and_changed_a();
        new_snap.tables.remove("b");

        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap();
        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        assert_eq!(
            parse_table_entry_kinds(&raw).get("b"),
            Some(&TableEntryKind::Dropped)
        );
    }

    #[test]
    fn a_table_absent_from_the_base_is_recorded_in_full() {
        let dir = crate::test_scratch::scratch_dir();
        let (mut base_snap, new_snap, registry) = make_base_and_changed_a();
        base_snap.tables.remove("b");

        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap();
        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        assert_eq!(
            parse_table_entry_kinds(&raw).get("b"),
            Some(&TableEntryKind::Full)
        );
    }

    /// `TableRegistry::register` refuses to re-register a name under a
    /// different concrete type, but a table can still end up recreated with
    /// a different `R`/`K` between two checkpoints: `WriteTx::delete_table`
    /// removes a name entirely, and `Store::register_table_keyed`'s guard
    /// only compares the *key* type against the live table
    /// (`src/store.rs:917`), not the record type `R`. If the base
    /// snapshot — kept alive in memory as `Arc<Snapshot>`, never reloaded
    /// from a file — still holds the table under its old type, `diff_table`'s
    /// base-side downcast fails with `Error::TableTypeChanged`, and this is
    /// the fallback that must fire: a `Full` entry carrying the *new* type's
    /// whole table, not a diff against the incomparable old one.
    #[test]
    fn a_table_recreated_with_a_different_type_is_recorded_in_full() {
        let dir = crate::test_scratch::scratch_dir();

        // Base holds "a" as a `Table<User>` — not registered under this name
        // at all, standing in for a type the current registry no longer
        // describes (the old registration is gone once the table was
        // dropped).
        let mut old_table = Table::<User>::new();
        old_table
            .insert(User {
                name: "Old".into(),
                age: 99,
            })
            .unwrap();
        let mut base_tables = std::collections::BTreeMap::new();
        base_tables.insert(
            "a".to_string(),
            std::sync::Arc::new(old_table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        let base_snap = Snapshot {
            version: 1,
            tables: base_tables,
        };

        // New snapshot and registry both describe "a" as `Table<Widget>` —
        // a different concrete record type recreated under the same name.
        let mut reg = TableRegistry::default();
        reg.register::<Widget, u64>("a").unwrap();
        let mut new_table = Table::<Widget>::new();
        new_table
            .insert(Widget {
                label: "fresh".into(),
            })
            .unwrap();
        let mut new_tables = std::collections::BTreeMap::new();
        new_tables.insert(
            "a".to_string(),
            std::sync::Arc::new(new_table) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        let new_snap = Snapshot {
            version: 2,
            tables: new_tables,
        };

        let v = write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &reg).unwrap();
        let raw = std::fs::read(dir.path().join(format!("checkpoint_{v}.bin"))).unwrap();
        let entries = parse_checkpoint_entries(&raw);
        let entry = entries.get("a").expect("table 'a' must have an entry");
        assert_eq!(entry.kind, TableEntryKind::Full);

        // The payload round-trips as a whole table of the *new* type, not a
        // delta against the incomparable old one.
        let info = reg.get("a").unwrap();
        let restored = (info.deserialize_table)(entry.payload.as_ref().unwrap()).unwrap();
        let restored = restored.as_any().downcast_ref::<Table<Widget>>().unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(
            restored.get(&1).unwrap(),
            &Widget {
                label: "fresh".into()
            }
        );
    }

    // -----------------------------------------------------------------------
    // find_head_chain / load_chain
    // -----------------------------------------------------------------------

    /// Write a base(v1) + delta(v2) + delta(v3) chain to `dir`: table "a"
    /// gains a row at v2 (via `make_base_and_changed_a`) and another at v3,
    /// "b" is untouched throughout. Returns the registry and the in-memory
    /// snapshot at v3 (the head), so tests can compare against it directly or
    /// corrupt/remove one of the three files on disk afterward.
    fn write_a_three_link_chain(dir: &Path) -> (TableRegistry, Snapshot) {
        let (base_snap, mid_snap, registry) = make_base_and_changed_a();
        write_checkpoint(dir, &base_snap, &registry).unwrap(); // v1: full
        write_delta_checkpoint(dir, &mid_snap, &base_snap, &registry).unwrap(); // v2: delta

        let mut table_a = mid_snap
            .tables
            .get("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<User>>()
            .unwrap()
            .clone();
        table_a
            .insert(User {
                name: "Dana".into(),
                age: 22,
            })
            .unwrap();
        let mut head_tables = mid_snap.tables.clone();
        head_tables.insert(
            "a".to_string(),
            std::sync::Arc::new(table_a) as std::sync::Arc<dyn crate::table::MergeableTable>,
        );
        let head_snap = Snapshot {
            version: 3,
            tables: head_tables,
        };
        write_delta_checkpoint(dir, &head_snap, &mid_snap, &registry).unwrap(); // v3: delta

        (registry, head_snap)
    }

    #[test]
    fn a_chain_recovers_to_the_same_state_as_a_full_checkpoint() {
        let dir = crate::test_scratch::scratch_dir();
        let (registry, head_snap) = write_a_three_link_chain(dir.path());

        let chain = find_head_chain(dir.path()).unwrap();
        assert_eq!(chain.len(), 3, "base + two deltas");
        let snap = load_chain(&chain, &registry).unwrap();
        assert_eq!(snap.version, 3);

        let a = snap
            .tables
            .get("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<User>>()
            .unwrap();
        let expected_a = head_snap
            .tables
            .get("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<User>>()
            .unwrap();
        assert_eq!(a.len(), expected_a.len());
        for (k, v) in expected_a.iter() {
            assert_eq!(a.get(k), Some(v));
        }

        let b = snap
            .tables
            .get("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Table<User>>()
            .unwrap();
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn a_corrupt_mid_chain_delta_fails_loudly() {
        let dir = crate::test_scratch::scratch_dir();
        let (registry, _head_snap) = write_a_three_link_chain(dir.path());

        let mut bytes = std::fs::read(dir.path().join("checkpoint_2.bin")).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        std::fs::write(dir.path().join("checkpoint_2.bin"), &bytes).unwrap();

        let chain = find_head_chain(dir.path()).unwrap();
        let err = load_chain(&chain, &registry).unwrap_err();
        assert!(
            matches!(err, Error::CheckpointCorrupted(_)),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn a_missing_mid_chain_delta_fails_loudly_rather_than_recovering_stale_data() {
        let dir = crate::test_scratch::scratch_dir();
        write_a_three_link_chain(dir.path());

        std::fs::remove_file(dir.path().join("checkpoint_2.bin")).unwrap();
        let err = find_head_chain(dir.path()).unwrap_err();
        assert!(
            matches!(err, Error::CheckpointChainBroken { head: 3, missing: 2 }),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn a_dropped_table_does_not_reappear_after_chain_replay() {
        let dir = crate::test_scratch::scratch_dir();
        let (base_snap, mut new_snap, registry) = make_base_and_changed_a();
        new_snap.tables.remove("b");
        write_checkpoint(dir.path(), &base_snap, &registry).unwrap(); // v1 full, a+b
        write_delta_checkpoint(dir.path(), &new_snap, &base_snap, &registry).unwrap(); // v2 delta, drops b

        let snap = load_chain(&find_head_chain(dir.path()).unwrap(), &registry).unwrap();
        assert!(!snap.tables.contains_key("b"));
    }

    #[test]
    fn write_checkpoint_dir_fsync_roundtrip() {
        let dir = crate::test_scratch::scratch_dir();
        let store = crate::Store::new(crate::StoreConfig {
            persistence: crate::Persistence::Smr {
                dir: dir.path().to_path_buf(),
            },
            ..crate::StoreConfig::default()
        })
        .unwrap();
        store.register_table::<String>("t").unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<String>("t").unwrap().insert("x".into()).unwrap();
            wtx.commit().unwrap();
        }
        let v = store.checkpoint().unwrap();
        assert!(dir.path().join(format!("checkpoint_{v}.bin")).exists());
    }
}
