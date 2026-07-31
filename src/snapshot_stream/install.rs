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

/// What to do with tables present in the destination snapshot but not in the
/// incoming wire stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnExtra {
    /// Preserve them in the new snapshot (default — backwards-compatible
    /// merge semantics).
    Keep,
    /// Drop them — the new snapshot exactly mirrors the wire stream.
    /// Required for Raft/SMR deployments where `InstallSnapshot` must make
    /// the follower's state identical to the leader's.
    Drop,
}

/// Options for [`Store::install_snapshot_stream`](crate::Store::install_snapshot_stream).
#[derive(Debug, Clone)]
pub struct InstallOptions {
    /// How to handle tables present in the stream but not registered in the
    /// destination store.
    pub on_unknown_tables: OnUnknown,
    /// How to handle tables present in the destination snapshot but absent
    /// from the wire stream. Defaults to [`OnExtra::Keep`] (merge); set to
    /// [`OnExtra::Drop`] to get strict replace semantics suitable for Raft
    /// `InstallSnapshot`.
    pub on_extra_tables: OnExtra,
    /// Pin the installed snapshot to an exact version. `None` (default) lands
    /// it at the next auto-assigned version (`> latest_version`). `Some(v)`
    /// installs it at version `v` and advances the internal counter so the next
    /// auto-assigned write gets `v + 1`. `v` must be **strictly greater** than
    /// the destination's current `latest_version`, or the install is refused
    /// with [`SnapshotStreamError::InvalidCommitVersion`]. Used by SMR
    /// deployments that want the snapshot version to match the log index (byte
    /// position) it was produced from — see `ultima_cluster`'s
    /// `StoreStateMachine` position-as-version invariant.
    pub commit_version: Option<u64>,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            on_unknown_tables: OnUnknown::Drop,
            on_extra_tables: OnExtra::Keep,
            commit_version: None,
        }
    }
}

// ---------------------------------------------------------------------------
// install_snapshot_stream — added to Store via impl block
// ---------------------------------------------------------------------------

impl crate::store::Store {
    /// Drain a snapshot wire stream produced by [`Store::snapshot_stream`](crate::Store::snapshot_stream),
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

        // Capture the registry, the destination's current snapshot, and the
        // base version atomically under a single read lock. Capturing these
        // separately races with concurrent committers: a write that lands
        // between reads would let `install_batch`'s OCC check pass while
        // `base_snapshot` still reflects the pre-commit state, silently
        // dropping any indexes added in the interim.
        let (registry, base_snapshot, base_version) = {
            let inner = self.inner.read();
            let v = inner.latest_version;
            let snap = inner.snapshots.get(&v).cloned();
            (Arc::clone(&inner.registry), snap, v)
        };

        // A caller-pinned `commit_version` must strictly exceed the current
        // latest — reject before decoding so a bad request is cheap and the
        // destination is provably untouched. The OCC check inside
        // `install_batch` re-validates `latest_version == base_version` under
        // the write lock, so if a concurrent commit races in between it fails
        // there (WriteConflict) rather than installing at a stale version.
        if let Some(v) = opts.commit_version
            && v <= base_version
        {
            return Err(SnapshotStreamError::InvalidCommitVersion {
                requested: v,
                latest: base_version,
            });
        }

        // ── 3. Per-table parsing ──────────────────────────────────────────
        let mut pending: Vec<PendingTable> = Vec::new();
        let mut stream_table_names: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        let mut declared_rows_total: u64 = 0;

        // Cap pre-allocation against untrusted `row_count`. Each row on the
        // wire is at minimum 12 bytes (u64 key + u32 val_len + 0-byte value),
        // so any legitimate `row_count` cannot exceed `remaining_bytes / 12`.
        // Without this, a malicious or corrupted `row_count = u64::MAX` would
        // request a many-GB allocation and abort the process before the CRC
        // check runs.
        const MIN_ROW_BYTES: usize = 12;

        for _ in 0..file_header.table_count {
            // 3a. Table header.
            let (table_header, n) = decode_table_header(&bytes[p..])?;
            total_crc.update(&bytes[p..p + n]);
            p += n;
            stream_table_names.insert(table_header.name.clone());
            declared_rows_total = declared_rows_total.saturating_add(table_header.row_count);

            // 3b. Rows: key(u64 LE) | val_len(u32 LE) | val(bytes).
            let mut table_crc = crc32fast::Hasher::new();
            let remaining = bytes.len().saturating_sub(p);
            let cap_hint =
                std::cmp::min(table_header.row_count as usize, remaining / MIN_ROW_BYTES);
            let mut rows: Vec<(u64, Vec<u8>)> = Vec::with_capacity(cap_hint);

            for _ in 0..table_header.row_count {
                if bytes.len() < p + 12 {
                    return Err(SnapshotStreamError::Truncated);
                }
                let key = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
                let val_len = u32::from_le_bytes(bytes[p + 8..p + 12].try_into().unwrap()) as usize;
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

            // The wire format's row keys are fixed 8-byte little-endian
            // `u64`s. If either the destination's registration *or* its live
            // table uses any other primary-key type, those bytes must NOT be
            // re-encoded into it: `String::decode` of `1u64`'s eight bytes
            // succeeds (NUL is valid UTF-8), the strict-ascent check
            // downstream still passes, and the table installs full of garbage
            // keys with no error anywhere. Refuse at the trust boundary
            // instead. (Task 7 makes the format key-generic and this
            // restriction goes away.)
            //
            // *Both* are checked because they can disagree, and each guards a
            // different failure. The registry's `K` decides what the rebuilt
            // table will be, so a non-`u64` registration mis-decodes the
            // incoming bytes. The live table's `K` is what a `u64` install
            // would silently *replace*: `build_from_raw_rows` looks the
            // existing table up through an `Option` chain
            // (`registry.rs`'s `.and_then(..downcast_ref::<Table<R, K>>())`),
            // so a mistyped destination degrades to "no index definitions"
            // rather than erroring, and the install lands a `u64` table on top
            // of a `String` one — flipping the key type and destroying every
            // pre-existing row without a word.
            let u64_key = std::any::TypeId::of::<u64>();
            let live_key = existing_table.map(|t| (t.key_type_id(), t.key_type_name()));
            let registered_key = registry.key_type(&table_header.name);
            let offending = [live_key, registered_key]
                .into_iter()
                .flatten()
                .find(|(id, _)| *id != u64_key);
            if let Some((_, key_type)) = offending {
                return Err(SnapshotStreamError::NonU64Key {
                    table: table_header.name,
                    key_type,
                });
            }

            // Reject any custom secondary index up-front: the index-rebuild
            // path inside `Table::from_bulk` panics for `IndexKind::Custom`
            // because there is no generic "make empty" hook. Convert that
            // latent panic into a clean error at this trust boundary.
            if let Some(et) = existing_table {
                for (kind_byte, idx_name) in et.index_list() {
                    if kind_byte == 2 {
                        return Err(SnapshotStreamError::CustomIndexUnsupported {
                            table: table_header.name.clone(),
                            index: idx_name,
                        });
                    }
                }
            }

            // The registry builds rows from order-preserving encoded key
            // bytes, but the stream format still carries fixed 8-byte
            // little-endian `u64` keys (the mirror image of the adapter in
            // `build.rs`). Re-encode here; task 7 makes the format
            // key-generic and both adapters go away.
            let rows: Vec<(Vec<u8>, Vec<u8>)> = rows
                .into_iter()
                .map(|(key, val)| (crate::primary_key::PrimaryKey::encode(&key), val))
                .collect();

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
        // `opts.commit_version` (validated above) pins the installed snapshot
        // to an exact version; `None` lands it at the next auto-assigned one.

        // Empty pending + Keep semantics → nothing changes; skip the snapshot
        // bump. With Drop semantics we must still install if extras would be
        // dropped, so fall through. Note a pinned `commit_version` has no
        // effect in this degenerate no-op case (there is nothing to install to
        // carry the version) — the SMR path always has a registered table with
        // state, so `pending` is non-empty in practice.
        if pending.is_empty() && opts.on_extra_tables == OnExtra::Keep {
            return Ok(base_version);
        }

        let new_version = match opts.on_extra_tables {
            OnExtra::Keep => self.install_batch(pending, base_version, opts.commit_version)?,
            OnExtra::Drop => {
                // Build keep-set of names that survived OnUnknown::Drop and
                // OnUnknown::Keep — anything in the wire stream that's not
                // forbidden by OnUnknown::Error policy. We track all
                // stream-table names regardless of registration; unregistered
                // ones are dropped from `pending` but still count as "in the
                // stream" for replace semantics.
                if pending.is_empty() {
                    // Drop policy with no installable tables: degenerate case
                    // — replace the snapshot with one containing only the
                    // surviving extras (i.e., none if all dst tables are
                    // declared in the stream). Without a pending table we
                    // have nothing to install via install_batch_replace, so
                    // just return base_version unchanged. Replicas should not
                    // hit this in practice (an empty stream would mean an
                    // empty leader state).
                    return Ok(base_version);
                }
                self.install_batch_replace(
                    pending,
                    base_version,
                    stream_table_names,
                    opts.commit_version,
                )?
            }
        };

        Ok(new_version)
    }
}

// ---------------------------------------------------------------------------
// Live-table key-type guards
// ---------------------------------------------------------------------------
//
// These live inside the crate because they need to construct a store whose
// registry and whose *live table* disagree about the primary key type. Both
// public routes into that state are now closed — `open_table_keyed` validates
// against the registry when it creates a table from nothing, and
// `register_table_keyed` validates against the latest snapshot — so the
// disagreement is forced here directly. That is the point: it proves the wire
// guards themselves read the live table, rather than passing only because
// something upstream happened to keep the two in step.

#[cfg(all(test, feature = "persistence"))]
mod live_key_type_tests {
    use std::io::{Cursor, Read};
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::{InstallOptions, SnapshotStreamError};
    use crate::store::Store;
    use crate::table::{MergeableTable, Table};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    struct Row {
        v: u64,
    }

    /// Overwrite `name`'s entry in the latest snapshot, bypassing every public
    /// guard. Used only to manufacture the registry/live-table disagreement
    /// the guards under test exist to catch.
    fn force_live_table(store: &Store, name: &str, table: Box<dyn MergeableTable>) {
        let mut inner = store.inner.write();
        let v = inner.latest_version;
        let mut snap = (*inner.snapshots[&v]).clone();
        snap.tables.insert(name.to_string(), Arc::from(table));
        inner.snapshots.insert(v, Arc::new(snap));
    }

    fn string_table(key: &str, v: u64) -> Box<dyn MergeableTable> {
        let mut t = Table::<Row, String>::new_keyed();
        t.put(key.to_string(), Row { v }).unwrap();
        Box::new(t)
    }

    /// Emit side. Registry says `u64`, the live table is `String`-keyed.
    /// Gating on the registry let this produce a stream in which
    /// `u64::decode` had reinterpreted the eight bytes of the key `"abcdefgh"`
    /// as `7017280452245743464` — a corrupt stream a clean destination would
    /// accept without complaint. The guard must read the table.
    #[test]
    fn emit_guard_reads_the_live_table_not_the_registry() {
        let store = Store::default();
        store.register_table::<Row>("t").unwrap();
        {
            let mut tx = store.begin_write(None).unwrap();
            tx.open_table::<Row>("t").unwrap().insert(Row { v: 1 }).unwrap();
            tx.commit().unwrap();
        }
        // Exactly 8 bytes, so `u64::decode` would happily accept it.
        force_live_table(&store, "t", string_table("abcdefgh", 7));

        let mut buf = Vec::new();
        let err = store
            .snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut buf)
            .expect_err("a String-keyed table must not be emitted as u64 rows");
        assert!(
            err.to_string().contains("non-u64 primary key"),
            "expected a non-u64 key refusal, got: {err}"
        );
        assert!(
            !buf.windows(8).any(|w| w == 7017280452245743464u64.to_le_bytes()),
            "no reinterpreted key may reach the wire"
        );
    }

    /// Install side. Registry says `u64`, the destination's live table is
    /// `String`-keyed. Gating on the registry let the install succeed,
    /// replacing the `String` table with a `u64` one — flipping the key type
    /// and destroying every pre-existing row, with no error. `build_from_raw_rows`
    /// cannot catch it: it looks the existing table up through an `Option`
    /// chain, so a mistyped destination degrades to "no index definitions".
    #[test]
    fn install_guard_reads_the_live_table_not_the_registry() {
        let src = Store::default();
        src.register_table::<Row>("t").unwrap();
        {
            let mut tx = src.begin_write(None).unwrap();
            tx.open_table::<Row>("t").unwrap().insert(Row { v: 1 }).unwrap();
            tx.commit().unwrap();
        }
        let mut bytes = Vec::new();
        src.snapshot_stream(None)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();

        let dst = Store::default();
        dst.register_table::<Row>("t").unwrap();
        force_live_table(&dst, "t", string_table("keep-me", 99));
        let before = dst.begin_read(None).unwrap().version();

        let res = dst.install_snapshot_stream(Cursor::new(&bytes), InstallOptions::default());
        match res {
            Err(SnapshotStreamError::NonU64Key { table, .. }) => assert_eq!(table, "t"),
            other => panic!("expected NonU64Key, got: {other:?}"),
        }

        // Destination untouched: same version, same String-keyed table, and
        // the row that the silent install destroyed is still there.
        let rtx = dst.begin_read(None).unwrap();
        assert_eq!(rtx.version(), before);
        assert!(
            rtx.open_table::<Row>("t").is_err(),
            "the table must still be String-keyed, not flipped to u64"
        );
        let t = rtx.open_table_keyed::<Row, String>("t").unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t.get("keep-me".to_string()), Some(&Row { v: 99 }));
    }
}
