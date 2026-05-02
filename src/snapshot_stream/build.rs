// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Build path: `SnapshotReader` — streams the wire format from a frozen
//! `Arc<Snapshot>` without blocking concurrent writers.
//!
//! State machine: `NotStarted → BetweenTables → InTable → Done`.
//!
//! `SnapshotReader` is gated on the `persistence` feature because row
//! serialization requires `serde + bincode` (via the type registry).

use std::io::{self, Read};
use std::sync::Arc;

use crate::registry::TableRegistry;
use crate::store::Snapshot;
use super::SnapshotStreamError;
use super::codec::{
    FILE_FORMAT_V, FILE_MAGIC, IndexDef, TableHeader, FileHeader,
    encode_file_header, encode_table_header,
};

// ---------------------------------------------------------------------------
// SnapshotReader — public streaming reader
// ---------------------------------------------------------------------------

/// A streaming snapshot reader that implements [`std::io::Read`].
///
/// Returned by [`Store::snapshot_stream`](crate::Store::snapshot_stream).
/// Emits the `ULTSNAP` wire format:
/// `[file header][per-table: (header, rows, table_crc32)][file trailer]`
///
/// Streaming is driven lazily — bytes are produced only as the caller drains
/// them through `Read::read`. The snapshot is frozen at construction time;
/// concurrent writes to the store do not affect the bytes emitted.
pub struct SnapshotReader {
    /// The frozen snapshot being streamed.
    snapshot: Arc<Snapshot>,
    /// Registry used to serialize row values to bincode bytes.
    registry: Arc<TableRegistry>,
    /// Small output buffer. Filled by `fill_buffer`; drained by `Read::read`.
    buffer: Vec<u8>,
    /// Read cursor into `buffer`.
    cursor: usize,
    /// Current state of the stream state machine.
    state: State,
    /// Sorted table names (deterministic order across replicas).
    table_names: Vec<String>,
    /// Index of the next table to start emitting.
    next_table_idx: usize,
    /// State for the table currently being emitted.
    current_table: Option<TableIterState>,
    /// CRC32 over everything emitted before the file trailer.
    total_crc: crc32fast::Hasher,
    /// Running count of total rows emitted (across all tables).
    total_rows: u64,
}

enum State {
    NotStarted,
    BetweenTables,
    InTable,
    Done,
}

struct TableIterState {
    /// Pre-serialized rows for the current table, in primary-key order.
    rows: Vec<(u64, Vec<u8>)>,
    /// Index of the next row to emit from `rows`.
    row_idx: usize,
    /// CRC32 over the row stream of this table only.
    table_crc: crc32fast::Hasher,
}

impl SnapshotReader {
    /// Construct a reader over the given frozen snapshot using the registry
    /// to serialize row values.
    pub(crate) fn new(
        snapshot: Arc<Snapshot>,
        registry: Arc<TableRegistry>,
    ) -> Result<Self, SnapshotStreamError> {
        // Build a sorted, stable iteration order.
        let mut table_names = snapshot.table_names();
        table_names.sort();
        Ok(Self {
            snapshot,
            registry,
            buffer: Vec::with_capacity(64 * 1024),
            cursor: 0,
            state: State::NotStarted,
            table_names,
            next_table_idx: 0,
            current_table: None,
            total_crc: crc32fast::Hasher::new(),
            total_rows: 0,
        })
    }

    /// Advance the state machine and fill `self.buffer` with the next chunk
    /// of bytes. Returns `Ok(())` when there is something to drain or when
    /// we've reached `Done`. Returns an error on serialization failure.
    fn fill_buffer(&mut self) -> Result<(), SnapshotStreamError> {
        self.buffer.clear();
        self.cursor = 0;

        match &self.state {
            State::NotStarted => {
                // Emit file header.
                let header = FileHeader {
                    format_ver: FILE_FORMAT_V,
                    store_ver: self.snapshot.version,
                    table_count: self.table_names.len() as u32,
                };
                encode_file_header(&header, &mut self.buffer);
                self.total_crc.update(&self.buffer);
                self.state = State::BetweenTables;
            }

            State::BetweenTables => {
                if self.next_table_idx >= self.table_names.len() {
                    // All tables done — emit file trailer.
                    // total_rows (u64 LE) + total_crc (u32 LE) + bookend magic (8 bytes)
                    self.buffer.extend_from_slice(&self.total_rows.to_le_bytes());
                    // total_crc covers everything up to (but not including) the
                    // trailer itself, so we finalize *before* writing the trailer.
                    let crc = self.total_crc.clone().finalize();
                    self.buffer.extend_from_slice(&crc.to_le_bytes());
                    self.buffer.extend_from_slice(FILE_MAGIC);
                    self.state = State::Done;
                    return Ok(());
                }

                let name = self.table_names[self.next_table_idx].clone();
                self.next_table_idx += 1;

                // Look up the table in the snapshot.
                let table_arc = match self.snapshot.tables.get(&name) {
                    Some(t) => t.clone(),
                    None => {
                        // Table was registered but has no snapshot entry yet —
                        // skip it (treat as empty rather than error).
                        return Ok(()); // re-enter BetweenTables next call
                    }
                };

                // Serialize all rows for this table via the registry.
                let serialize_fn = self
                    .registry
                    .get(&name)
                    .map(|info| &*info.serialize_record)
                    .ok_or_else(|| SnapshotStreamError::UnknownTable {
                        name: name.clone(),
                        type_id: 0,
                    })?;

                let serialized_rows = table_arc
                    .collect_serialized_rows(serialize_fn)
                    .map_err(|e| SnapshotStreamError::Bulk(e.to_string()))?;

                // Build index definitions for the table header.
                let index_list = table_arc.index_list();
                let indexes: Vec<IndexDef> = index_list
                    .into_iter()
                    .map(|(kind, name)| IndexDef { kind, name })
                    .collect();

                let record_type_id = self.registry.numeric_type_id(&name);
                let row_count = serialized_rows.len() as u64;

                let table_header = TableHeader {
                    name: name.clone(),
                    record_type_id,
                    row_count,
                    indexes,
                };
                encode_table_header(&table_header, &mut self.buffer);
                self.total_crc.update(&self.buffer);

                self.current_table = Some(TableIterState {
                    rows: serialized_rows,
                    row_idx: 0,
                    table_crc: crc32fast::Hasher::new(),
                });
                self.state = State::InTable;
            }

            State::InTable => {
                let ts = self.current_table.as_mut().unwrap();

                if ts.row_idx < ts.rows.len() {
                    // Emit one row: key (u64 LE) + val_len (u32 LE) + val bytes.
                    let (key, val) = &ts.rows[ts.row_idx];
                    ts.row_idx += 1;
                    self.total_rows += 1;

                    let val_len = val.len() as u32;
                    // Accumulate into a temporary chunk so we can CRC it in one pass.
                    let chunk_len = 8 + 4 + val.len();
                    self.buffer.reserve(chunk_len);
                    let start = self.buffer.len();
                    self.buffer.extend_from_slice(&key.to_le_bytes());
                    self.buffer.extend_from_slice(&val_len.to_le_bytes());
                    self.buffer.extend_from_slice(val);
                    let chunk = &self.buffer[start..];
                    ts.table_crc.update(chunk);
                    self.total_crc.update(chunk);
                } else {
                    // Table exhausted — emit table_crc32 trailer.
                    let crc = ts.table_crc.clone().finalize();
                    self.buffer.extend_from_slice(&crc.to_le_bytes());
                    self.total_crc.update(&crc.to_le_bytes());
                    self.current_table = None;
                    self.state = State::BetweenTables;
                }
            }

            State::Done => {
                // Nothing more to emit.
            }
        }

        Ok(())
    }
}

impl Read for SnapshotReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            // Drain the buffer first.
            if self.cursor < self.buffer.len() {
                let available = self.buffer.len() - self.cursor;
                let n = available.min(out.len());
                out[..n].copy_from_slice(&self.buffer[self.cursor..self.cursor + n]);
                self.cursor += n;
                return Ok(n);
            }

            // Buffer empty — check if we're done.
            if matches!(self.state, State::Done) {
                return Ok(0);
            }

            // Advance state machine to produce more bytes.
            self.fill_buffer()
                .map_err(|e| io::Error::other(e.to_string()))?;

            // If fill_buffer produced nothing (e.g. skipped table) and we're
            // not done yet, loop again to try the next state transition.
        }
    }
}
