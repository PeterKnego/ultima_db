// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Type registry for persistence.
//!
//! When persistence is enabled, the store needs to serialize and deserialize
//! tables without knowing the concrete record type at compile time. The
//! [`TableRegistry`] stores type-erased serializer/deserializer closures
//! registered via [`TableRegistry::register`].

#![allow(dead_code)]

use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::persistence::Record;
use crate::primary_key::{PrimaryKey, auto_counter_seed};
use crate::table::{MergeableTable, Table};
use crate::{Error, Result};

// Deserialize and new-empty closures return `Box<dyn MergeableTable>`
// because their output flows directly into `Snapshot.tables`. Replay
// closures take `&mut dyn Any` because callers pass through
// `MergeableTable::as_any_mut()` — keeping `dyn Any` here avoids pulling
// MergeableTable into every closure's body.
//
// Row keys cross this boundary as `PrimaryKey::encode` bytes, never as
// `u64`: the concrete key type is erased here, and the encoding is
// order-preserving, so byte order is key order for every caller that needs
// to sort or range over them.
type SerializeAnyFn = Box<dyn Fn(&dyn Any) -> Result<Vec<u8>> + Send + Sync>;
type DeserializeRecordFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>> + Send + Sync>;
type DeserializeTableFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn MergeableTable>> + Send + Sync>;
type ReplayInsertFn = Box<dyn Fn(&mut dyn Any, &[u8], &[u8]) -> Result<()> + Send + Sync>;
type ReplayUpdateFn = Box<dyn Fn(&mut dyn Any, &[u8], &[u8]) -> Result<()> + Send + Sync>;
type ReplayDeleteFn = Box<dyn Fn(&mut dyn Any, &[u8]) -> Result<()> + Send + Sync>;
type NewEmptyTableFn = Box<dyn Fn() -> Box<dyn MergeableTable> + Send + Sync>;
/// Build a `Table<R, K>` from a list of raw (encoded-key, bincode-bytes) pairs
/// and wrap it as `Box<dyn MergeableTable>`. Used by `install_snapshot_stream`
/// to reconstruct a table from the wire format without `R`/`K` bounds at the
/// call site.
///
/// `existing` is the destination's current table by the same name, if any. The
/// closure downcasts it to `&Table<R, K>` and clones its `empty_index_defs()`
/// so secondary indexes survive the install (they're rebuilt from the new rows
/// via `Table::from_bulk`'s `rebuild_from_sorted_data` pass). When `None`
/// (fresh receiver with no prior table), the new table has no indexes.
type BuildFromRawRowsFn = Box<
    dyn Fn(Vec<(Vec<u8>, Vec<u8>)>, Option<&dyn MergeableTable>) -> Result<Box<dyn MergeableTable>>
        + Send
        + Sync,
>;

/// Type-erased serialization functions for a single table type.
pub(crate) struct TableTypeInfo {
    /// `TypeId` of the concrete record type `R`, used to validate that
    /// `open_table::<R>` matches the registered type.
    pub type_id: TypeId,
    /// `TypeId` of the concrete primary-key type `K`.
    ///
    /// Every closure below downcasts to `Table<R, K>`, so `R` alone does not
    /// identify what this entry can operate on: registering `("users", User,
    /// u64)` and then `("users", User, String)` differ only here. Without the
    /// key type in the identity check the second registration is a silent
    /// no-op that only surfaces much later — at checkpoint or replay time — as
    /// an opaque "table downcast failed". It is also what lets the snapshot
    /// wire-format install path refuse a `u64`-keyed stream aimed at a
    /// non-`u64` destination table instead of decoding 8 raw bytes into a
    /// garbage key.
    pub key_type_id: TypeId,
    /// `std::any::type_name::<K>()`, carried purely so a key-type mismatch
    /// can name the offending type in its error message (`TypeId` has no
    /// printable form).
    pub key_type_name: &'static str,
    /// Serialize a single record (`&R` as `&dyn Any`) to bytes.
    pub serialize_record: SerializeAnyFn,
    /// Deserialize bytes to a single record (`Box<dyn Any + Send + Sync>` wrapping `R`).
    pub deserialize_record: DeserializeRecordFn,
    /// Serialize an entire `Table<R, K>` (as `&dyn Any`) to bytes.
    pub serialize_table: SerializeAnyFn,
    /// Deserialize bytes to a `Table<R, K>` (as `Box<dyn MergeableTable>`).
    pub deserialize_table: DeserializeTableFn,
    /// Create a new empty `Table<R, K>` as `Box<dyn Any + Send + Sync>`.
    pub new_empty_table: NewEmptyTableFn,
    /// Insert a serialized record into a `Table<R, K>` (as `&mut dyn Any`) at
    /// an encoded key.
    pub replay_insert: ReplayInsertFn,
    /// Update a record in a `Table<R, K>` (as `&mut dyn Any`) with serialized
    /// data, addressed by encoded key.
    pub replay_update: ReplayUpdateFn,
    /// Delete a record from a `Table<R, K>` (as `&mut dyn Any`) by encoded key.
    pub replay_delete: ReplayDeleteFn,
    /// Deserialize raw `(encoded key, bytes)` pairs and build a fresh
    /// `Table<R, K>`.
    /// If a destination table by the same name already exists, its index
    /// definitions are cloned and rebuilt over the new rows so secondary
    /// indexes survive the install.
    pub build_from_raw_rows: BuildFromRawRowsFn,
}

/// Registry mapping table names to their type-erased serializers.
///
/// Populated at store creation time via [`Store::register_table`](crate::Store::register_table).
#[derive(Default)]
pub(crate) struct TableRegistry {
    entries: BTreeMap<String, TableTypeInfo>,
}

/// Hex-render a key for an error message. `K` carries no `Debug`/`Display`
/// bound; the order-preserving encoding is the one printable form every key
/// type has (`Table::insert_with_id` reports duplicates the same way).
fn hex<K: PrimaryKey>(key: &K) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("0x");
    for b in key.encode() {
        let _ = write!(out, "{b:02x}");
    }
    out
}

impl TableRegistry {
    /// Register a table type. Stores closures that can serialize/deserialize
    /// records and full tables for the record type `R` keyed by `K`.
    pub fn register<R: Record, K: PrimaryKey>(&mut self, name: &str) -> Result<()> {
        use std::collections::btree_map::Entry;
        match self.entries.entry(name.to_string()) {
            Entry::Occupied(e) => {
                // Both halves of the identity must match: a same-`R`,
                // different-`K` re-registration is a *conflict*, not an
                // idempotent repeat (the stored closures downcast to
                // `Table<R, K>` and would reject every table the caller
                // subsequently opens).
                if e.get().type_id != TypeId::of::<R>()
                    || e.get().key_type_id != TypeId::of::<K>()
                {
                    return Err(Error::TypeMismatch(name.to_string()));
                }
                // Already registered with the same record and key type — no-op.
                Ok(())
            }
            Entry::Vacant(e) => {
                e.insert(TableTypeInfo {
                    type_id: TypeId::of::<R>(),
                    key_type_id: TypeId::of::<K>(),
                    key_type_name: std::any::type_name::<K>(),
                    serialize_record: Box::new(|any| {
                        let record = any
                            .downcast_ref::<R>()
                            .ok_or_else(|| Error::TypeMismatch("record downcast failed".into()))?;
                        bincode::serde::encode_to_vec(record, bincode::config::standard())
                            .map_err(|e| Error::Persistence(e.to_string()))
                    }),
                    deserialize_record: Box::new(|bytes| {
                        let (record, _): (R, _) =
                            bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                                .map_err(|e| Error::Persistence(e.to_string()))?;
                        Ok(Box::new(record))
                    }),
                    serialize_table: Box::new(|any| {
                        let table = any
                            .downcast_ref::<Table<R, K>>()
                            .ok_or_else(|| Error::TypeMismatch("table downcast failed".into()))?;
                        serialize_table(table)
                    }),
                    deserialize_table: Box::new(|bytes| {
                        let table = deserialize_table::<R, K>(bytes)?;
                        Ok(Box::new(table))
                    }),
                    new_empty_table: Box::new(|| {
                        Box::new(Table::<R, K>::empty_with_counter(auto_counter_seed::<K>()))
                    }),
                    replay_insert: Box::new(|table_any, key, data| {
                        let table = table_any.downcast_mut::<Table<R, K>>().ok_or_else(|| {
                            Error::TypeMismatch("replay_insert downcast failed".into())
                        })?;
                        let key = K::decode(key)?;
                        let (record, _): (R, _) =
                            bincode::serde::decode_from_slice(data, bincode::config::standard())
                                .map_err(|e| Error::Persistence(e.to_string()))?;
                        // `Table::insert_with_id` carries an `AutoKey` bound
                        // (it advances the id counter), so the erased path
                        // spells its duplicate check itself and writes through
                        // `put`, which maintains indexes and advances the
                        // counter only on `AutoKey` tables.
                        if table.get(&key).is_some() {
                            return Err(Error::DuplicateKey(format!("key {}", hex(&key))));
                        }
                        table.put(key, record)?;
                        Ok(())
                    }),
                    replay_update: Box::new(|table_any, key, data| {
                        let table = table_any.downcast_mut::<Table<R, K>>().ok_or_else(|| {
                            Error::TypeMismatch("replay_update downcast failed".into())
                        })?;
                        let key = K::decode(key)?;
                        let (record, _): (R, _) =
                            bincode::serde::decode_from_slice(data, bincode::config::standard())
                                .map_err(|e| Error::Persistence(e.to_string()))?;
                        table.update(&key, record)?;
                        Ok(())
                    }),
                    replay_delete: Box::new(|table_any, key| {
                        let table = table_any.downcast_mut::<Table<R, K>>().ok_or_else(|| {
                            Error::TypeMismatch("replay_delete downcast failed".into())
                        })?;
                        let key = K::decode(key)?;
                        table.delete(&key)?;
                        Ok(())
                    }),
                    build_from_raw_rows: Box::new(|raw_rows, existing| {
                        let config = bincode::config::standard();
                        let mut sorted: Vec<(K, Arc<R>)> = Vec::with_capacity(raw_rows.len());
                        for (key_bytes, bytes) in raw_rows {
                            let key = K::decode(&key_bytes)?;
                            let (record, _): (R, _) =
                                bincode::serde::decode_from_slice(&bytes, config)
                                    .map_err(|e| Error::Persistence(e.to_string()))?;
                            sorted.push((key, Arc::new(record)));
                        }
                        // Validate strictly-ascending unique keys at this trust
                        // boundary. `BTree::from_sorted` only debug-asserts, so
                        // a malformed wire payload would silently corrupt the
                        // tree in release builds.
                        for (i, w) in sorted.windows(2).enumerate() {
                            if w[0].0 >= w[1].0 {
                                return Err(Error::Persistence(format!(
                                    "rows not strictly sorted: key {} at row {i} >= key {} at \
                                     row {}",
                                    hex(&w[0].0),
                                    hex(&w[1].0),
                                    i + 1
                                )));
                            }
                        }
                        // Auto-increment counter: seed with the `AutoKey` start
                        // value (`u64` only) and advance it past the highest
                        // key. `advance_auto_counter` is a no-op for every
                        // explicitly-keyed type, so those tables land on `None`
                        // — the invariant the v2 `has_next_id` byte encodes.
                        let mut next_id = auto_counter_seed::<K>();
                        let has_counter = next_id.is_some();
                        if let Some((last, _)) = sorted.last() {
                            last.advance_auto_counter(&mut next_id);
                        }
                        // The counter is cleared only when the highest key has
                        // no successor (`u64::MAX`). Dropping the `Some`
                        // invariant would turn a later `insert` into a panic —
                        // fail loudly here instead.
                        if has_counter && next_id.is_none() {
                            return Err(Error::Persistence(
                                "last row id is u64::MAX; next_id would overflow".into(),
                            ));
                        }
                        // Preserve secondary indexes by cloning the destination's
                        // existing index definitions. Empty for a fresh receiver.
                        let index_defs = existing
                            .and_then(|t| t.as_any().downcast_ref::<Table<R, K>>())
                            .map(|t| t.empty_index_defs())
                            .transpose()?
                            .unwrap_or_default();
                        let table = Table::<R, K>::from_bulk(sorted, next_id, index_defs)?;
                        Ok(Box::new(table))
                    }),
                });
                Ok(())
            }
        }
    }

    /// Returns a stable-within-process `u64` identifier for the registered
    /// type of `table_name`. Used by `SnapshotReader` to populate the
    /// `record_type_id` field in the wire-format table header.
    ///
    /// The value is derived by hashing the Rust `TypeId` with a deterministic
    /// `DefaultHasher` seeded from 0. It is stable within a single process run
    /// but not guaranteed stable across Rust versions — treat it as a best-effort
    /// type-identity hint for install-time mismatch detection, not a persistent ID.
    pub fn numeric_type_id(&self, name: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        match self.entries.get(name) {
            None => 0,
            Some(info) => {
                let mut h = DefaultHasher::new();
                info.type_id.hash(&mut h);
                h.finish()
            }
        }
    }

    /// Look up the type info for a table by name.
    pub fn get(&self, name: &str) -> Option<&TableTypeInfo> {
        self.entries.get(name)
    }

    /// Check if a table is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Validate that a table name is registered with the expected record type
    /// and the expected primary-key type.
    ///
    /// Both halves matter: the registry's closures are built for one exact
    /// `Table<R, K>`, so a table opened as `Table<R, u64>` against an entry
    /// registered for `Table<R, String>` would serialize and replay through
    /// closures that reject it.
    pub fn validate_type_keyed<R: 'static, K: 'static>(&self, name: &str) -> Result<()> {
        if let Some(info) = self.entries.get(name)
            && (info.type_id != TypeId::of::<R>() || info.key_type_id != TypeId::of::<K>())
        {
            return Err(Error::TypeMismatch(name.to_string()));
        }
        // If not registered, we don't enforce — allows non-persistent tables.
        Ok(())
    }

    /// [`validate_type_keyed`](Self::validate_type_keyed) for the default
    /// `u64`-keyed table.
    pub fn validate_type<R: 'static>(&self, name: &str) -> Result<()> {
        self.validate_type_keyed::<R, u64>(name)
    }

    /// The registered primary-key type for `name` as `(TypeId, type_name)`,
    /// or `None` if the table is not registered. Used by the snapshot-stream
    /// build/install paths, whose wire format still carries fixed 8-byte
    /// `u64` keys and must refuse anything else rather than reinterpret it.
    pub fn key_type(&self, name: &str) -> Option<(TypeId, &'static str)> {
        self.entries
            .get(name)
            .map(|info| (info.key_type_id, info.key_type_name))
    }

    /// Returns the names of all registered tables.
    pub fn table_names(&self) -> Vec<&str> {
        self.entries.keys().map(|s| s.as_str()).collect()
    }

    /// Deserialize a list of raw `(key, bytes)` pairs and build a fresh
    /// `Box<dyn MergeableTable>` for the named table. Returns `None` if the
    /// table is not registered.
    ///
    /// Used by `Store::install_snapshot_stream` to reconstruct each table
    /// from the snapshot wire format without requiring a generic `R` at the
    /// call site.
    pub fn build_table_from_raw(
        &self,
        name: &str,
        raw_rows: Vec<(Vec<u8>, Vec<u8>)>,
        existing: Option<&dyn MergeableTable>,
    ) -> Option<Result<Box<dyn MergeableTable>>> {
        let info = self.entries.get(name)?;
        Some((info.build_from_raw_rows)(raw_rows, existing))
    }
}

// ---------------------------------------------------------------------------
// Table serialization — format v2
// ---------------------------------------------------------------------------

/// Leading byte of a v2 payload, and the reason v1 can never be mistaken for
/// v2.
///
/// A v1 payload opened with bincode's *varint* encoding of `next_id`, so its
/// first byte is the varint tag: `0..=250` for a small `next_id`, or one of
/// the width markers `251..=254`. `0xFF` is not a legal bincode varint tag, so
/// no v1 payload can start with it. A bare version byte of `2` would not have
/// been enough — a v1 table whose `next_id` was `2` (any table that took
/// exactly one insert) encodes its first byte as `0x02`.
const TABLE_MAGIC_V2: u8 = 0xFF;

/// Format version carried in the second header byte.
const TABLE_FORMAT_V2: u8 = 2;

/// Serialize a `Table<R, K>` to bytes.
///
/// Format v2:
/// `[magic: u8 = 0xFF][version: u8 = 2][has_next_id: u8]`
/// `[next_id_len: u32-be, next_id_bytes]?`
/// `[num_entries: u64-be]`
/// `[key_len: u32-be, key_bytes, rec_len: u32-be, rec_bytes]*`
///
/// All lengths are big-endian and explicit: `PrimaryKey::encode` is variable-
/// length for `String`, `Vec<u8>` and tuples, so the v1 assumption of a fixed
/// 8-byte row id no longer holds. `has_next_id` is `0` exactly for an
/// explicitly-keyed table (`next_id_opt() == None`) and `1` for an
/// auto-increment one.
///
/// v1 (`[next_id: u64][count: u64][id: u64, rec]*`, all bincode-varint) is
/// rejected on read — see [`TABLE_MAGIC_V2`].
fn serialize_table<R: Record, K: PrimaryKey>(table: &Table<R, K>) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    let mut buf = Vec::new();

    buf.push(TABLE_MAGIC_V2);
    buf.push(TABLE_FORMAT_V2);

    match table.next_id_opt() {
        Some(id) => {
            buf.push(1u8);
            let enc = id.encode();
            buf.extend_from_slice(&(enc.len() as u32).to_be_bytes());
            buf.extend_from_slice(&enc);
        }
        None => buf.push(0u8),
    }

    buf.extend_from_slice(&(table.len() as u64).to_be_bytes());

    // `Table::iter` walks the B-tree in ascending key order, which is what
    // `from_bulk` needs on the way back in.
    for (key, record) in table.iter() {
        let kb = key.encode();
        buf.extend_from_slice(&(kb.len() as u32).to_be_bytes());
        buf.extend_from_slice(&kb);
        let rb = bincode::serde::encode_to_vec(record, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        buf.extend_from_slice(&(rb.len() as u32).to_be_bytes());
        buf.extend_from_slice(&rb);
    }

    Ok(buf)
}

/// Read `n` bytes at `at`, advancing it. Every length in the payload is
/// attacker-controlled, so the arithmetic is checked.
fn take<'a>(bytes: &'a [u8], at: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = at.checked_add(n).filter(|e| *e <= bytes.len()).ok_or_else(|| {
        Error::Persistence(format!(
            "table payload truncated: need {n} bytes at offset {at}, have {}",
            bytes.len().saturating_sub(*at)
        ))
    })?;
    let out = &bytes[*at..end];
    *at = end;
    Ok(out)
}

fn take_u32(bytes: &[u8], at: &mut usize) -> Result<u32> {
    Ok(u32::from_be_bytes(take(bytes, at, 4)?.try_into().unwrap()))
}

fn take_u64(bytes: &[u8], at: &mut usize) -> Result<u64> {
    Ok(u64::from_be_bytes(take(bytes, at, 8)?.try_into().unwrap()))
}

/// Deserialize a `Table<R, K>` from bytes written by [`serialize_table`].
fn deserialize_table<R: Record, K: PrimaryKey>(bytes: &[u8]) -> Result<Table<R, K>> {
    let config = bincode::config::standard();
    let mut at = 0usize;

    let header = take(bytes, &mut at, 2)?;
    if header[0] != TABLE_MAGIC_V2 {
        return Err(Error::Persistence(format!(
            "unsupported table format version: leading byte 0x{:02x} is not the v{} marker \
             (this looks like a pre-0.3.0 v1 table, whose row keys were fixed 8-byte ids). \
             Table data written before 0.3.0 cannot be read by this build. To migrate: with \
             the previous UltimaDB version, `Store::recover()` the old directory, read the \
             rows out through a `ReadTx`, and load them into a 0.3.0+ store with \
             `Store::bulk_load` / `Store::bulk_load_batch` (or move them over the wire with \
             `Store::open_checkpoint_reader` + `Store::install_snapshot_stream`). To discard \
             the old data instead, delete the checkpoint and WAL files in the persistence \
             directory.",
            header[0], TABLE_FORMAT_V2
        )));
    }
    if header[1] != TABLE_FORMAT_V2 {
        return Err(Error::Persistence(format!(
            "unsupported table format version {}: this build reads v{}. The data was written \
             by a newer UltimaDB — upgrade the binary to read it.",
            header[1], TABLE_FORMAT_V2
        )));
    }

    let next_id = match take(bytes, &mut at, 1)?[0] {
        0 => None,
        1 => {
            let len = take_u32(bytes, &mut at)? as usize;
            Some(K::decode(take(bytes, &mut at, len)?)?)
        }
        other => {
            return Err(Error::Persistence(format!(
                "invalid has_next_id byte {other} in table payload"
            )));
        }
    };

    let count = take_u64(bytes, &mut at)?;
    // `count` is attacker-controlled; every row costs at least 8 bytes on the
    // wire (two length prefixes), so clamp the pre-allocation to what the
    // payload could actually hold.
    let cap = (count as usize).min(bytes.len() / 8);
    let mut rows: Vec<(K, Arc<R>)> = Vec::with_capacity(cap);
    for _ in 0..count {
        let klen = take_u32(bytes, &mut at)? as usize;
        let key = K::decode(take(bytes, &mut at, klen)?)?;
        let rlen = take_u32(bytes, &mut at)? as usize;
        let (record, _): (R, _) =
            bincode::serde::decode_from_slice(take(bytes, &mut at, rlen)?, config)
                .map_err(|e| Error::Persistence(e.to_string()))?;
        rows.push((key, Arc::new(record)));
    }

    // The payload must be consumed exactly. Without this, a corrupted-but-not-
    // truncated payload is accepted silently: understating `count` (2 → 0)
    // yields an empty table with a live `next_id`, and appended junk is
    // ignored outright. Callers hand us CRC-verified, `data_len`-delimited
    // slices, so this is defense in depth — but "validate at the trust
    // boundary" is this function's job, and every other malformed-input check
    // here catches truncation, i.e. the opposite direction.
    if at != bytes.len() {
        return Err(Error::Persistence(format!(
            "table payload has {} trailing bytes after {count} rows",
            bytes.len() - at
        )));
    }

    // `from_bulk` only debug-asserts ascending order — validate it here, at
    // the trust boundary, so a malformed payload cannot corrupt the tree in a
    // release build.
    for (i, w) in rows.windows(2).enumerate() {
        if w[0].0 >= w[1].0 {
            return Err(Error::Persistence(format!(
                "table payload rows not strictly ascending: key {} at row {i} >= key {} at row {}",
                hex(&w[0].0),
                hex(&w[1].0),
                i + 1
            )));
        }
    }

    Table::from_bulk(rows, next_id, Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestUser {
        name: String,
        age: u32,
    }

    #[test]
    fn register_and_serialize_record() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();

        let user = TestUser {
            name: "Alice".into(),
            age: 30,
        };
        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_record)(&user).unwrap();
        let any = (info.deserialize_record)(&bytes).unwrap();
        let recovered = any.downcast_ref::<TestUser>().unwrap();
        assert_eq!(recovered, &user);
    }

    #[test]
    fn register_and_serialize_table() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();

        let mut table = Table::<TestUser>::new();
        table
            .insert(TestUser {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        table
            .insert(TestUser {
                name: "Bob".into(),
                age: 25,
            })
            .unwrap();

        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_table)(&table).unwrap();
        let any = (info.deserialize_table)(&bytes).unwrap();
        let recovered = any.as_any().downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(
            recovered.get(&1).unwrap(),
            &TestUser {
                name: "Alice".into(),
                age: 30
            }
        );
        assert_eq!(
            recovered.get(&2).unwrap(),
            &TestUser {
                name: "Bob".into(),
                age: 25
            }
        );
    }

    #[test]
    fn register_duplicate_same_type_is_noop() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        reg.register::<TestUser, u64>("users").unwrap(); // no error
    }

    #[test]
    fn register_duplicate_different_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        assert!(matches!(
            reg.register::<String, u64>("users"),
            Err(Error::TypeMismatch(_))
        ));
    }

    /// Same record type, different *key* type is a conflict, not an
    /// idempotent repeat. Without the `key_type_id` half of the identity
    /// check the second call would silently no-op, leaving the registry
    /// holding `Table<TestUser, u64>` closures for a table every caller
    /// afterwards opens as `Table<TestUser, String>` — a mismatch that only
    /// surfaces at checkpoint or replay time as "table downcast failed".
    #[test]
    fn register_duplicate_same_record_different_key_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        assert!(matches!(
            reg.register::<TestUser, String>("users"),
            Err(Error::TypeMismatch(_))
        ));
        // The first registration is intact — the failed one changed nothing.
        assert_eq!(
            reg.key_type("users").map(|(id, _)| id),
            Some(TypeId::of::<u64>())
        );
        reg.register::<TestUser, u64>("users").unwrap();
    }

    #[test]
    fn register_duplicate_same_record_and_key_is_noop() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, String>("users").unwrap();
        reg.register::<TestUser, String>("users").unwrap(); // no error
        assert_eq!(
            reg.key_type("users").map(|(id, _)| id),
            Some(TypeId::of::<String>())
        );
    }

    #[test]
    fn validate_type_keyed_rejects_wrong_key_type() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, String>("users").unwrap();
        reg.validate_type_keyed::<TestUser, String>("users").unwrap();
        assert!(matches!(
            reg.validate_type_keyed::<TestUser, u64>("users"),
            Err(Error::TypeMismatch(_))
        ));
        // The `u64` convenience wrapper agrees.
        assert!(matches!(
            reg.validate_type::<TestUser>("users"),
            Err(Error::TypeMismatch(_))
        ));
    }

    #[test]
    fn validate_type_matches() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        reg.validate_type::<TestUser>("users").unwrap();
    }

    #[test]
    fn validate_type_mismatch() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        assert!(matches!(
            reg.validate_type::<String>("users"),
            Err(Error::TypeMismatch(_))
        ));
    }

    #[test]
    fn validate_unregistered_passes() {
        let reg = TableRegistry::default();
        reg.validate_type::<TestUser>("unknown").unwrap(); // no error
    }

    #[test]
    fn contains_registered_table() {
        let mut reg = TableRegistry::default();
        assert!(!reg.contains("users"));
        reg.register::<TestUser, u64>("users").unwrap();
        assert!(reg.contains("users"));
        assert!(!reg.contains("orders"));
    }

    #[test]
    fn table_names_returns_all_registered() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        reg.register::<String, u64>("logs").unwrap();
        let mut names = reg.table_names();
        names.sort();
        assert_eq!(names, vec!["logs", "users"]);
    }

    #[test]
    fn table_names_empty_registry() {
        let reg = TableRegistry::default();
        assert!(reg.table_names().is_empty());
    }

    #[test]
    fn new_empty_table_creates_empty() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();
        let table_any = (info.new_empty_table)();
        let table = table_any
            .as_any()
            .downcast_ref::<Table<TestUser>>()
            .unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn replay_insert_update_delete() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        // Start with an empty table
        let mut table_box: Box<dyn MergeableTable> = (info.new_empty_table)();

        // Replay insert
        let user = TestUser {
            name: "Alice".into(),
            age: 30,
        };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        (info.replay_insert)(table_box.as_any_mut(), &1u64.encode(), &data).unwrap();

        let table = table_box
            .as_any()
            .downcast_ref::<Table<TestUser>>()
            .unwrap();
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&1).unwrap().name, "Alice");

        // Replay update
        let updated = TestUser {
            name: "Alice Updated".into(),
            age: 31,
        };
        let data = bincode::serde::encode_to_vec(&updated, bincode::config::standard()).unwrap();
        (info.replay_update)(table_box.as_any_mut(), &1u64.encode(), &data).unwrap();

        let table = table_box
            .as_any()
            .downcast_ref::<Table<TestUser>>()
            .unwrap();
        assert_eq!(table.get(&1).unwrap().name, "Alice Updated");

        // Replay delete
        (info.replay_delete)(table_box.as_any_mut(), &1u64.encode()).unwrap();

        let table = table_box
            .as_any()
            .downcast_ref::<Table<TestUser>>()
            .unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn replay_insert_duplicate_id_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table_box: Box<dyn MergeableTable> = (info.new_empty_table)();
        let user = TestUser {
            name: "Alice".into(),
            age: 30,
        };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        (info.replay_insert)(table_box.as_any_mut(), &1u64.encode(), &data).unwrap();
        // Insert same ID again
        let result = (info.replay_insert)(table_box.as_mut(), &1u64.encode(), &data);
        assert!(result.is_err());
    }

    #[test]
    fn replay_update_missing_id_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table_box: Box<dyn MergeableTable> = (info.new_empty_table)();
        let user = TestUser {
            name: "Alice".into(),
            age: 30,
        };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        let result = (info.replay_update)(table_box.as_mut(), &99u64.encode(), &data);
        assert!(result.is_err());
    }

    #[test]
    fn replay_delete_missing_id_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table_box: Box<dyn MergeableTable> = (info.new_empty_table)();
        let result = (info.replay_delete)(table_box.as_mut(), &99u64.encode());
        assert!(result.is_err());
    }

    #[test]
    fn serialize_record_wrong_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();
        // Pass a String instead of TestUser
        let wrong: String = "not a user".into();
        let result = (info.serialize_record)(&wrong);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn serialize_table_wrong_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();
        // Pass a Table<String> instead of Table<TestUser>
        let wrong_table = Table::<String>::new();
        let result = (info.serialize_table)(&wrong_table);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn replay_insert_wrong_table_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut wrong_table: Box<dyn Any + Send + Sync> = Box::new(Table::<String>::new());
        let user = TestUser {
            name: "Alice".into(),
            age: 30,
        };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        let result = (info.replay_insert)(wrong_table.as_mut(), &1u64.encode(), &data);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn replay_update_wrong_table_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut wrong_table: Box<dyn Any + Send + Sync> = Box::new(Table::<String>::new());
        let user = TestUser {
            name: "Alice".into(),
            age: 30,
        };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        let result = (info.replay_update)(wrong_table.as_mut(), &1u64.encode(), &data);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn replay_delete_wrong_table_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut wrong_table: Box<dyn Any + Send + Sync> = Box::new(Table::<String>::new());
        let result = (info.replay_delete)(wrong_table.as_mut(), &1u64.encode());
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn serialize_deserialize_empty_table() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();

        let table = Table::<TestUser>::new();
        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_table)(&table).unwrap();
        let any = (info.deserialize_table)(&bytes).unwrap();
        let recovered = any.as_any().downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(recovered.len(), 0);
        assert_eq!(recovered.next_id(), 1);
    }

    #[test]
    fn table_roundtrip_preserves_next_id() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();

        let mut table = Table::<TestUser>::new();
        table
            .insert(TestUser {
                name: "A".into(),
                age: 1,
            })
            .unwrap();
        table
            .insert(TestUser {
                name: "B".into(),
                age: 2,
            })
            .unwrap();
        table.delete(&2).unwrap(); // next_id stays at 3

        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_table)(&table).unwrap();
        let any = (info.deserialize_table)(&bytes).unwrap();
        let recovered = any.as_any().downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered.next_id(), 3);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let reg = TableRegistry::default();
        assert!(reg.get("nope").is_none());
    }

    /// The point of format v2: a table whose primary key is not a `u64` at
    /// all round-trips through the type-erased serializer.
    #[test]
    fn roundtrip_string_keyed_table() {
        let mut reg = TableRegistry::default();
        reg.register::<String, String>("emails").unwrap();

        let mut t: Table<String, String> = Table::new_keyed();
        t.put("a@x.com".to_string(), "Alice".to_string()).unwrap();
        t.put("b@x.com".to_string(), "Bob".to_string()).unwrap();

        let info = reg.get("emails").unwrap();
        let bytes = (info.serialize_table)(&t as &dyn std::any::Any).unwrap();
        let restored = (info.deserialize_table)(&bytes).unwrap();
        let restored = restored
            .as_any()
            .downcast_ref::<Table<String, String>>()
            .unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(
            restored.get(&"a@x.com".to_string()),
            Some(&"Alice".to_string())
        );
        assert_eq!(restored.get(&"b@x.com".to_string()), Some(&"Bob".to_string()));
        // An explicitly-keyed table has no id counter, and the `has_next_id`
        // byte must carry that across the round-trip.
        assert_eq!(restored.next_id_opt(), None);
    }

    #[test]
    fn roundtrip_u64_keyed_table_preserves_next_id() {
        let mut reg = TableRegistry::default();
        reg.register::<String, u64>("rows").unwrap();

        let mut t: Table<String> = Table::new();
        t.insert("first".to_string()).unwrap();
        t.insert("second".to_string()).unwrap();

        let info = reg.get("rows").unwrap();
        let bytes = (info.serialize_table)(&t as &dyn std::any::Any).unwrap();
        let restored = (info.deserialize_table)(&bytes).unwrap();
        let restored = restored.as_any().downcast_ref::<Table<String>>().unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored.next_id(), 3);
    }

    #[test]
    fn deserialize_rejects_v1_format() {
        // A v1 payload began with the bincode varint of `next_id`, whose
        // leading byte is never the v2 magic (0xFF is not a legal varint tag).
        let mut reg = TableRegistry::default();
        reg.register::<String, u64>("rows").unwrap();
        let info = reg.get("rows").unwrap();

        let v1_bytes = vec![0u8; 24];
        let Err(err) = (info.deserialize_table)(&v1_bytes) else {
            panic!("a v1 payload must be rejected");
        };
        assert!(
            format!("{err}").contains("format version"),
            "expected a format-version error, got: {err}"
        );
    }

    /// The ambiguity a bare `[version: u8 = 2]` header would have had: a real
    /// v1 payload for a table with exactly one insert starts with the byte
    /// `0x02` (varint of `next_id = 2`). It must still be rejected.
    #[test]
    fn deserialize_rejects_v1_payload_whose_first_byte_is_two() {
        let config = bincode::config::standard();
        let mut v1 = Vec::new();
        bincode::encode_into_std_write(2u64, &mut v1, config).unwrap(); // next_id
        bincode::encode_into_std_write(1u64, &mut v1, config).unwrap(); // count
        bincode::encode_into_std_write(1u64, &mut v1, config).unwrap(); // row id
        bincode::serde::encode_into_std_write(
            &TestUser {
                name: "Alice".into(),
                age: 30,
            },
            &mut v1,
            config,
        )
        .unwrap();
        assert_eq!(v1[0], TABLE_FORMAT_V2, "fixture must exercise the collision");

        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();
        let Err(err) = (info.deserialize_table)(&v1) else {
            panic!("a v1 payload must be rejected");
        };
        assert!(
            format!("{err}").contains("format version"),
            "expected a format-version error, got: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_a_newer_format_version() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut bytes = vec![TABLE_MAGIC_V2, TABLE_FORMAT_V2 + 1, 0u8];
        bytes.extend_from_slice(&0u64.to_be_bytes());
        let Err(err) = (info.deserialize_table)(&bytes) else {
            panic!("a newer format version must be rejected");
        };
        assert!(
            format!("{err}").contains("format version"),
            "expected a format-version error, got: {err}"
        );
    }

    /// Length fields are attacker-controlled: a truncated payload must error,
    /// never panic on an out-of-range slice.
    #[test]
    fn deserialize_truncated_payload_errors_not_panics() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table = Table::<TestUser>::new();
        table
            .insert(TestUser {
                name: "Alice".into(),
                age: 30,
            })
            .unwrap();
        let full = (info.serialize_table)(&table).unwrap();
        for cut in 1..full.len() {
            assert!(
                (info.deserialize_table)(&full[..cut]).is_err(),
                "truncation at {cut} bytes must error"
            );
        }
    }

    /// Build a two-row `u64` payload and hand back its bytes. Also pins the
    /// v2 header layout the corruption tests index into.
    fn two_row_payload(info: &TableTypeInfo) -> Vec<u8> {
        let mut table = Table::<TestUser>::new();
        for (name, age) in [("Alice", 30u32), ("Bob", 25)] {
            table
                .insert(TestUser {
                    name: name.into(),
                    age,
                })
                .unwrap();
        }
        let bytes = (info.serialize_table)(&table).unwrap();
        // [magic][version][has_next_id][next_id_len: u32-be][next_id: 8][count: u64-be]
        assert_eq!(bytes[0], TABLE_MAGIC_V2);
        assert_eq!(bytes[1], TABLE_FORMAT_V2);
        assert_eq!(bytes[2], 1, "a u64 table carries a counter");
        assert_eq!(u32::from_be_bytes(bytes[3..7].try_into().unwrap()), 8);
        assert_eq!(u64::from_be_bytes(bytes[7..15].try_into().unwrap()), 3);
        assert_eq!(u64::from_be_bytes(bytes[COUNT_AT..COUNT_AT + 8].try_into().unwrap()), 2);
        bytes
    }

    /// Offset of the `num_entries` field for a `u64`-keyed payload.
    const COUNT_AT: usize = 15;

    /// Corrupted-but-*not*-truncated input: understating `count` must not be
    /// read as a smaller table. Before the trailing-byte check this returned
    /// an empty table carrying a live `next_id == 3` and no error at all —
    /// every other malformed-input test here is a prefix truncation, which is
    /// why the gap survived.
    #[test]
    fn deserialize_rejects_understated_row_count() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut bytes = two_row_payload(info);
        bytes[COUNT_AT..COUNT_AT + 8].copy_from_slice(&0u64.to_be_bytes());

        let Err(err) = (info.deserialize_table)(&bytes) else {
            panic!("an understated row count must be rejected");
        };
        assert!(
            format!("{err}").contains("trailing bytes"),
            "expected a trailing-bytes error, got: {err}"
        );
    }

    /// The other direction: bytes appended after the last row.
    #[test]
    fn deserialize_rejects_trailing_junk() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut bytes = two_row_payload(info);
        bytes.extend_from_slice(&[0xABu8; 32]);

        let Err(err) = (info.deserialize_table)(&bytes) else {
            panic!("trailing junk must be rejected");
        };
        assert!(
            format!("{err}").contains("trailing bytes"),
            "expected a trailing-bytes error, got: {err}"
        );
    }

    /// The rejection message must point at APIs that exist.
    #[test]
    fn v1_rejection_message_names_a_real_migration_path() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let Err(err) = (info.deserialize_table)(&[0u8; 24]) else {
            panic!("a v1 payload must be rejected");
        };
        let msg = format!("{err}");
        for expected in ["format version", "Store::recover", "bulk_load", "delete"] {
            assert!(msg.contains(expected), "message missing {expected:?}: {msg}");
        }
    }

    /// The wire order guarantee `Table::from_bulk` only debug-asserts.
    #[test]
    fn build_from_raw_rows_rejects_unsorted_keys() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let rec = bincode::serde::encode_to_vec(
            TestUser {
                name: "A".into(),
                age: 1,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let rows = vec![
            (2u64.encode(), rec.clone()),
            (1u64.encode(), rec.clone()),
        ];
        assert!(matches!(
            (info.build_from_raw_rows)(rows, None),
            Err(Error::Persistence(_))
        ));
    }

    /// An empty install of a `u64` table must still hand back a usable id
    /// counter — `Table::insert` panics without one.
    #[test]
    fn build_from_raw_rows_empty_u64_table_keeps_its_counter() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();

        let built = (info.build_from_raw_rows)(Vec::new(), None).unwrap();
        let table = built.as_any().downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(table.next_id(), 1);
    }

    /// ...and an explicitly-keyed table must never grow one.
    #[test]
    fn build_from_raw_rows_keyed_table_has_no_counter() {
        let mut reg = TableRegistry::default();
        reg.register::<String, String>("emails").unwrap();
        let info = reg.get("emails").unwrap();

        let config = bincode::config::standard();
        let rec = bincode::serde::encode_to_vec("Alice".to_string(), config).unwrap();
        let built =
            (info.build_from_raw_rows)(vec![("a@x.com".to_string().encode(), rec)], None).unwrap();
        let table = built
            .as_any()
            .downcast_ref::<Table<String, String>>()
            .unwrap();
        assert_eq!(table.len(), 1);
        assert_eq!(table.next_id_opt(), None);
    }

    /// `new_empty_table` is the WAL-replay path for a table absent from the
    /// base snapshot; it obeys the same counter invariant.
    #[test]
    fn new_empty_table_counter_follows_the_key_type() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        reg.register::<String, String>("emails").unwrap();

        let auto = (reg.get("users").unwrap().new_empty_table)();
        assert_eq!(
            auto.as_any()
                .downcast_ref::<Table<TestUser>>()
                .unwrap()
                .next_id(),
            1
        );

        let keyed = (reg.get("emails").unwrap().new_empty_table)();
        assert_eq!(
            keyed
                .as_any()
                .downcast_ref::<Table<String, String>>()
                .unwrap()
                .next_id_opt(),
            None
        );
    }

    /// Replay addresses rows by encoded key, so a non-`u64` key type replays
    /// too.
    #[test]
    fn replay_roundtrip_on_a_string_keyed_table() {
        let mut reg = TableRegistry::default();
        reg.register::<String, String>("emails").unwrap();
        let info = reg.get("emails").unwrap();

        let mut table_box: Box<dyn MergeableTable> = (info.new_empty_table)();
        let key = "a@x.com".to_string().encode();
        let config = bincode::config::standard();
        let data = bincode::serde::encode_to_vec("Alice".to_string(), config).unwrap();
        (info.replay_insert)(table_box.as_any_mut(), &key, &data).unwrap();
        // Duplicate insert at the same key is still an error.
        assert!((info.replay_insert)(table_box.as_any_mut(), &key, &data).is_err());

        let updated = bincode::serde::encode_to_vec("Alice 2".to_string(), config).unwrap();
        (info.replay_update)(table_box.as_any_mut(), &key, &updated).unwrap();
        {
            let table = table_box
                .as_any()
                .downcast_ref::<Table<String, String>>()
                .unwrap();
            assert_eq!(table.get(&"a@x.com".to_string()), Some(&"Alice 2".into()));
            assert_eq!(table.next_id_opt(), None);
        }

        (info.replay_delete)(table_box.as_any_mut(), &key).unwrap();
        assert_eq!(
            table_box
                .as_any()
                .downcast_ref::<Table<String, String>>()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn deserialize_record_bad_bytes_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser, u64>("users").unwrap();
        let info = reg.get("users").unwrap();
        let result = (info.deserialize_record)(&[0xFF, 0xFF, 0xFF]);
        assert!(matches!(result, Err(Error::Persistence(_))));
    }
}
