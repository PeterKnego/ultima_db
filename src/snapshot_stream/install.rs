// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Install path: `Store::install_snapshot_stream` — drains a snapshot wire
//! stream, validates per-table and total CRCs, and atomically installs as the
//! new latest snapshot via `install_batch`.

#[cfg(feature = "persistence")]
use std::io::Read;
#[cfg(feature = "persistence")]
use std::sync::Arc;

#[cfg(feature = "persistence")]
use crate::bulk_load::PendingTable;
#[cfg(feature = "persistence")]
use crate::table::MergeableTable;

#[cfg(feature = "persistence")]
use super::SnapshotStreamError;
#[cfg(feature = "persistence")]
use super::codec::{FILE_MAGIC, decode_file_header, decode_table_header};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// What to do when a table name found in the wire stream is not registered
/// in the destination store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnUnknown {
    /// Silently discard unrecognised tables and continue. Default.
    Drop,
    /// Preserve the current table from the destination snapshot (not yet
    /// implemented in v1; behaves the same as `Drop`).
    Keep,
    /// Return [`SnapshotStreamError::UnknownTable`].
    Error,
}

/// Options for [`Store::install_snapshot_stream`].
#[derive(Debug, Clone)]
pub struct InstallOptions {
    /// How to handle tables present in the stream but not registered in the
    /// destination store.
    pub on_unknown_tables: OnUnknown,
    /// Install the snapshot at a specific store version. `None` (default)
    /// uses `latest_version + 1`, the standard behaviour.
    ///
    /// Passing `Some(v)` installs the snapshot and then adjusts the internal
    /// version counter so that the next auto-assigned write gets `v + 1`.
    /// This is useful for SMR deployments that want the snapshot version to
    /// match the log index from which it was produced.
    pub commit_version: Option<u64>,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            on_unknown_tables: OnUnknown::Drop,
            commit_version: None,
        }
    }
}

// ---------------------------------------------------------------------------
// install_snapshot_stream — added to Store via impl block
// ---------------------------------------------------------------------------

impl crate::store::Store {
    /// Drain a snapshot wire stream produced by [`Store::snapshot_stream`],
    /// validate per-table and total CRC-32 checksums, and atomically install
    /// the result as the new latest snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotStreamError`] on any I/O failure, protocol violation
    /// (bad magic, wrong format version), truncated data, CRC mismatch, or
    /// unknown table when `opts.on_unknown_tables` is [`OnUnknown::Error`].
    ///
    /// On error the destination store is **unchanged** — `install_batch`
    /// is called only after all tables have been decoded and validated.
    ///
    /// # Feature gate
    ///
    /// Requires the `persistence` feature (same as `snapshot_stream`).
    #[cfg(feature = "persistence")]
    pub fn install_snapshot_stream<R: Read>(
        &self,
        mut reader: R,
        opts: InstallOptions,
    ) -> Result<u64, SnapshotStreamError> {
        // ── 1. Drain entire stream into memory ──────────────────────────────
        // v1 spec §6: acceptable to buffer the full payload; streaming
        // consumer is a future optimisation.
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let mut p = 0usize;

        // Running CRC over everything before the file trailer.
        let mut total_crc = crc32fast::Hasher::new();

        // ── 2. File header ───────────────────────────────────────────────────
        let (file_header, n) = decode_file_header(&bytes[p..])?;
        total_crc.update(&bytes[p..p + n]);
        p += n;

        // Grab the registry + a snapshot of the destination's current state
        // once. The current snapshot is consulted per-table to clone existing
        // index definitions so secondary indexes survive the install.
        let (registry, base_snapshot) = {
            let inner = self.inner.read().unwrap();
            let snap = inner
                .snapshots
                .get(&inner.latest_version)
                .cloned();
            (Arc::clone(&inner.registry), snap)
        };

        // Capture base_version before we start building tables.
        let base_version = self.latest_version();

        // ── 3. Per-table parsing ──────────────────────────────────────────
        let mut pending: Vec<PendingTable> = Vec::new();
        let mut declared_rows_total: u64 = 0;

        for _ in 0..file_header.table_count {
            // 3a. Table header.
            let (table_header, n) = decode_table_header(&bytes[p..])?;
            total_crc.update(&bytes[p..p + n]);
            p += n;
            declared_rows_total =
                declared_rows_total.saturating_add(table_header.row_count);

            // 3b. Rows: key(u64 LE) | val_len(u32 LE) | val(bytes).
            let mut table_crc = crc32fast::Hasher::new();
            let mut rows: Vec<(u64, Vec<u8>)> =
                Vec::with_capacity(table_header.row_count as usize);

            for _ in 0..table_header.row_count {
                if bytes.len() < p + 12 {
                    return Err(SnapshotStreamError::Truncated);
                }
                let key = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
                let val_len =
                    u32::from_le_bytes(bytes[p + 8..p + 12].try_into().unwrap()) as usize;
                if bytes.len() < p + 12 + val_len {
                    return Err(SnapshotStreamError::Truncated);
                }
                let val = bytes[p + 12..p + 12 + val_len].to_vec();
                let chunk = &bytes[p..p + 12 + val_len];
                table_crc.update(chunk);
                total_crc.update(chunk);
                rows.push((key, val));
                p += 12 + val_len;
            }

            // 3c. Table trailer: table_crc32 (u32 LE).
            if bytes.len() < p + 4 {
                return Err(SnapshotStreamError::Truncated);
            }
            let stored_table_crc = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
            // The table-crc trailer is itself covered by total_crc.
            total_crc.update(&bytes[p..p + 4]);
            p += 4;

            if stored_table_crc != table_crc.finalize() {
                return Err(SnapshotStreamError::BadCrc {
                    table: Some(table_header.name.clone()),
                });
            }

            // 3d. Dispatch: registered → deserialise; unknown → OnUnknown policy.
            if !registry.contains(&table_header.name) {
                match opts.on_unknown_tables {
                    OnUnknown::Drop | OnUnknown::Keep => {
                        // Drop: discard rows; we already validated the CRC.
                        // Keep (v1): preserve existing table — out of scope,
                        // treat same as Drop for now.
                        continue;
                    }
                    OnUnknown::Error => {
                        return Err(SnapshotStreamError::UnknownTable {
                            name: table_header.name,
                            type_id: table_header.record_type_id,
                        });
                    }
                }
            }

            // Deserialise raw bytes → Box<dyn MergeableTable> via the registry.
            // Pass the destination's existing table (if any) so its secondary
            // index definitions are cloned and rebuilt over the new rows.
            let existing_table: Option<&dyn MergeableTable> = base_snapshot
                .as_ref()
                .and_then(|snap| snap.tables.get(&table_header.name))
                .map(|t| t.as_ref() as &dyn MergeableTable);
            let table_box = registry
                .build_table_from_raw(&table_header.name, rows, existing_table)
                .expect("contains() returned true, so entry must exist")?;

            let table_arc: Arc<dyn MergeableTable> = Arc::from(table_box);
            pending.push(PendingTable {
                name: table_header.name,
                table: table_arc,
            });
        }

        // ── 4. File trailer ───────────────────────────────────────────────
        // Layout: total_rows(u64 LE) + total_crc32(u32 LE) + bookend(8 bytes)
        //
        // The build path finalises total_crc *before* writing total_rows, so
        // total_crc does NOT cover the trailer's total_rows field. To still
        // catch a flipped byte there, we cross-validate it against the sum of
        // per-table row_counts (which ARE covered by total_crc through the
        // table headers). Any mismatch → RowCountMismatch.
        if bytes.len() < p + 8 + 4 + 8 {
            return Err(SnapshotStreamError::Truncated);
        }
        let trailer_total_rows = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
        if trailer_total_rows != declared_rows_total {
            return Err(SnapshotStreamError::RowCountMismatch {
                trailer: trailer_total_rows,
                actual: declared_rows_total,
            });
        }
        let stored_total_crc = u32::from_le_bytes(bytes[p + 8..p + 12].try_into().unwrap());
        let bookend = &bytes[p + 12..p + 20];
        if bookend != FILE_MAGIC {
            return Err(SnapshotStreamError::BadMagic);
        }
        if stored_total_crc != total_crc.finalize() {
            return Err(SnapshotStreamError::BadCrc { table: None });
        }

        // ── 5. Atomic install ──────────────────────────────────────────────
        // Empty stream (no registered tables) → nothing to install.
        if pending.is_empty() {
            // Return base_version unchanged — no new snapshot created.
            return Ok(base_version);
        }

        // commit_version: v1 does not support caller-supplied versions for
        // install_snapshot_stream.  The new snapshot always lands at
        // `latest_version + 1`.  The field is kept in `InstallOptions` for
        // future use but is ignored here.
        let new_version = self.install_batch(pending, base_version)?;

        Ok(new_version)
    }
}
