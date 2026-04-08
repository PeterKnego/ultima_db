//! Type registry for persistence.
//!
//! When persistence is enabled, the store needs to serialize and deserialize
//! tables without knowing the concrete record type at compile time. The
//! [`TableRegistry`] stores type-erased serializer/deserializer closures
//! registered via [`TableRegistry::register`].

#![allow(dead_code)]

use std::any::{Any, TypeId};
use std::collections::BTreeMap;

use crate::persistence::Record;
use crate::table::Table;
use crate::{Error, Result};

type SerializeAnyFn = Box<dyn Fn(&dyn Any) -> Result<Vec<u8>> + Send + Sync>;
type DeserializeAnyFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn Any>> + Send + Sync>;
type ReplayInsertFn = Box<dyn Fn(&mut dyn Any, u64, &[u8]) -> Result<()> + Send + Sync>;
type ReplayUpdateFn = Box<dyn Fn(&mut dyn Any, u64, &[u8]) -> Result<()> + Send + Sync>;
type ReplayDeleteFn = Box<dyn Fn(&mut dyn Any, u64) -> Result<()> + Send + Sync>;
type NewEmptyTableFn = Box<dyn Fn() -> Box<dyn Any> + Send + Sync>;

/// Type-erased serialization functions for a single table type.
pub(crate) struct TableTypeInfo {
    /// `TypeId` of the concrete record type `R`, used to validate that
    /// `open_table::<R>` matches the registered type.
    pub type_id: TypeId,
    /// Serialize a single record (`&R` as `&dyn Any`) to bytes.
    pub serialize_record: SerializeAnyFn,
    /// Deserialize bytes to a single record (`Box<dyn Any>` wrapping `R`).
    pub deserialize_record: DeserializeAnyFn,
    /// Serialize an entire `Table<R>` (as `&dyn Any`) to bytes.
    pub serialize_table: SerializeAnyFn,
    /// Deserialize bytes to a `Table<R>` (as `Box<dyn Any>`).
    pub deserialize_table: DeserializeAnyFn,
    /// Create a new empty `Table<R>` as `Box<dyn Any>`.
    pub new_empty_table: NewEmptyTableFn,
    /// Insert a serialized record into a `Table<R>` (as `&mut dyn Any`) at a specific ID.
    pub replay_insert: ReplayInsertFn,
    /// Update a record in a `Table<R>` (as `&mut dyn Any`) with serialized data.
    pub replay_update: ReplayUpdateFn,
    /// Delete a record from a `Table<R>` (as `&mut dyn Any`) by ID.
    pub replay_delete: ReplayDeleteFn,
}

/// Registry mapping table names to their type-erased serializers.
///
/// Populated at store creation time via [`Store::register_table`](crate::Store::register_table).
#[derive(Default)]
pub(crate) struct TableRegistry {
    entries: BTreeMap<String, TableTypeInfo>,
}

impl TableRegistry {
    /// Register a table type. Stores closures that can serialize/deserialize
    /// records and full tables for the given record type `R`.
    pub fn register<R: Record>(&mut self, name: &str) -> Result<()> {
        use std::collections::btree_map::Entry;
        match self.entries.entry(name.to_string()) {
            Entry::Occupied(e) => {
                if e.get().type_id != TypeId::of::<R>() {
                    return Err(Error::TypeMismatch(name.to_string()));
                }
                // Already registered with the same type — no-op.
                Ok(())
            }
            Entry::Vacant(e) => {
                e.insert(TableTypeInfo {
                    type_id: TypeId::of::<R>(),
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
                            .downcast_ref::<Table<R>>()
                            .ok_or_else(|| Error::TypeMismatch("table downcast failed".into()))?;
                        serialize_table(table)
                    }),
                    deserialize_table: Box::new(|bytes| {
                        let table = deserialize_table::<R>(bytes)?;
                        Ok(Box::new(table))
                    }),
                    new_empty_table: Box::new(|| Box::new(Table::<R>::new())),
                    replay_insert: Box::new(|table_any, id, data| {
                        let table = table_any.downcast_mut::<Table<R>>()
                            .ok_or_else(|| Error::TypeMismatch("replay_insert downcast failed".into()))?;
                        let (record, _): (R, _) =
                            bincode::serde::decode_from_slice(data, bincode::config::standard())
                                .map_err(|e| Error::Persistence(e.to_string()))?;
                        table.insert_with_id(id, record)?;
                        Ok(())
                    }),
                    replay_update: Box::new(|table_any, id, data| {
                        let table = table_any.downcast_mut::<Table<R>>()
                            .ok_or_else(|| Error::TypeMismatch("replay_update downcast failed".into()))?;
                        let (record, _): (R, _) =
                            bincode::serde::decode_from_slice(data, bincode::config::standard())
                                .map_err(|e| Error::Persistence(e.to_string()))?;
                        table.update(id, record)?;
                        Ok(())
                    }),
                    replay_delete: Box::new(|table_any, id| {
                        let table = table_any.downcast_mut::<Table<R>>()
                            .ok_or_else(|| Error::TypeMismatch("replay_delete downcast failed".into()))?;
                        table.delete(id)?;
                        Ok(())
                    }),
                });
                Ok(())
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

    /// Validate that a table name is registered with the expected type.
    pub fn validate_type<R: 'static>(&self, name: &str) -> Result<()> {
        if let Some(info) = self.entries.get(name)
            && info.type_id != TypeId::of::<R>()
        {
            return Err(Error::TypeMismatch(name.to_string()));
        }
        // If not registered, we don't enforce — allows non-persistent tables.
        Ok(())
    }

    /// Returns the names of all registered tables.
    pub fn table_names(&self) -> Vec<&str> {
        self.entries.keys().map(|s| s.as_str()).collect()
    }
}

/// Serialize a `Table<R>` to bytes.
///
/// Format: `[next_id: u64][num_entries: u64][id: u64, record_bytes: ...]*`
fn serialize_table<R: Record>(table: &Table<R>) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    let mut buf = Vec::new();

    // next_id
    bincode::encode_into_std_write(table.next_id(), &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    // num entries
    let count = table.len() as u64;
    bincode::encode_into_std_write(count, &mut buf, config)
        .map_err(|e| Error::Persistence(e.to_string()))?;

    // entries: id + record
    for (id, record) in table.iter() {
        bincode::encode_into_std_write(id, &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        bincode::serde::encode_into_std_write(record, &mut buf, config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
    }

    Ok(buf)
}

/// Deserialize a `Table<R>` from bytes.
fn deserialize_table<R: Record>(bytes: &[u8]) -> Result<Table<R>> {
    let config = bincode::config::standard();
    let mut offset = 0;

    let (next_id, read): (u64, _) =
        bincode::decode_from_slice(&bytes[offset..], config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
    offset += read;

    let (count, read): (u64, _) =
        bincode::decode_from_slice(&bytes[offset..], config)
            .map_err(|e| Error::Persistence(e.to_string()))?;
    offset += read;

    let mut table = Table::<R>::new();
    for _ in 0..count {
        let (id, read): (u64, _) =
            bincode::decode_from_slice(&bytes[offset..], config)
                .map_err(|e| Error::Persistence(e.to_string()))?;
        offset += read;

        let (record, read): (R, _) =
            bincode::serde::decode_from_slice(&bytes[offset..], config)
                .map_err(|e| Error::Persistence(e.to_string()))?;
        offset += read;

        table.insert_with_id(id, record)?;
    }
    table.set_next_id(next_id);

    Ok(table)
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
        reg.register::<TestUser>("users").unwrap();

        let user = TestUser { name: "Alice".into(), age: 30 };
        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_record)(&user).unwrap();
        let any = (info.deserialize_record)(&bytes).unwrap();
        let recovered = any.downcast_ref::<TestUser>().unwrap();
        assert_eq!(recovered, &user);
    }

    #[test]
    fn register_and_serialize_table() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();

        let mut table = Table::<TestUser>::new();
        table.insert(TestUser { name: "Alice".into(), age: 30 }).unwrap();
        table.insert(TestUser { name: "Bob".into(), age: 25 }).unwrap();

        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_table)(&table).unwrap();
        let any = (info.deserialize_table)(&bytes).unwrap();
        let recovered = any.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered.get(1).unwrap(), &TestUser { name: "Alice".into(), age: 30 });
        assert_eq!(recovered.get(2).unwrap(), &TestUser { name: "Bob".into(), age: 25 });
    }

    #[test]
    fn register_duplicate_same_type_is_noop() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        reg.register::<TestUser>("users").unwrap(); // no error
    }

    #[test]
    fn register_duplicate_different_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        assert!(matches!(reg.register::<String>("users"), Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn validate_type_matches() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        reg.validate_type::<TestUser>("users").unwrap();
    }

    #[test]
    fn validate_type_mismatch() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        assert!(matches!(reg.validate_type::<String>("users"), Err(Error::TypeMismatch(_))));
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
        reg.register::<TestUser>("users").unwrap();
        assert!(reg.contains("users"));
        assert!(!reg.contains("orders"));
    }

    #[test]
    fn table_names_returns_all_registered() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        reg.register::<String>("logs").unwrap();
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
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();
        let table_any = (info.new_empty_table)();
        let table = table_any.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn replay_insert_update_delete() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        // Start with an empty table
        let mut table_box: Box<dyn Any> = (info.new_empty_table)();

        // Replay insert
        let user = TestUser { name: "Alice".into(), age: 30 };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        (info.replay_insert)(table_box.as_mut(), 1, &data).unwrap();

        let table = table_box.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(1).unwrap().name, "Alice");

        // Replay update
        let updated = TestUser { name: "Alice Updated".into(), age: 31 };
        let data = bincode::serde::encode_to_vec(&updated, bincode::config::standard()).unwrap();
        (info.replay_update)(table_box.as_mut(), 1, &data).unwrap();

        let table = table_box.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(table.get(1).unwrap().name, "Alice Updated");

        // Replay delete
        (info.replay_delete)(table_box.as_mut(), 1).unwrap();

        let table = table_box.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn replay_insert_duplicate_id_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table_box: Box<dyn Any> = (info.new_empty_table)();
        let user = TestUser { name: "Alice".into(), age: 30 };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        (info.replay_insert)(table_box.as_mut(), 1, &data).unwrap();
        // Insert same ID again
        let result = (info.replay_insert)(table_box.as_mut(), 1, &data);
        assert!(result.is_err());
    }

    #[test]
    fn replay_update_missing_id_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table_box: Box<dyn Any> = (info.new_empty_table)();
        let user = TestUser { name: "Alice".into(), age: 30 };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        let result = (info.replay_update)(table_box.as_mut(), 99, &data);
        assert!(result.is_err());
    }

    #[test]
    fn replay_delete_missing_id_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut table_box: Box<dyn Any> = (info.new_empty_table)();
        let result = (info.replay_delete)(table_box.as_mut(), 99);
        assert!(result.is_err());
    }

    #[test]
    fn serialize_record_wrong_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();
        // Pass a String instead of TestUser
        let wrong: String = "not a user".into();
        let result = (info.serialize_record)(&wrong);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn serialize_table_wrong_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();
        // Pass a Table<String> instead of Table<TestUser>
        let wrong_table = Table::<String>::new();
        let result = (info.serialize_table)(&wrong_table);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn replay_insert_wrong_table_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut wrong_table: Box<dyn Any> = Box::new(Table::<String>::new());
        let user = TestUser { name: "Alice".into(), age: 30 };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        let result = (info.replay_insert)(wrong_table.as_mut(), 1, &data);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn replay_update_wrong_table_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut wrong_table: Box<dyn Any> = Box::new(Table::<String>::new());
        let user = TestUser { name: "Alice".into(), age: 30 };
        let data = bincode::serde::encode_to_vec(&user, bincode::config::standard()).unwrap();
        let result = (info.replay_update)(wrong_table.as_mut(), 1, &data);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn replay_delete_wrong_table_type_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();

        let mut wrong_table: Box<dyn Any> = Box::new(Table::<String>::new());
        let result = (info.replay_delete)(wrong_table.as_mut(), 1);
        assert!(matches!(result, Err(Error::TypeMismatch(_))));
    }

    #[test]
    fn serialize_deserialize_empty_table() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();

        let table = Table::<TestUser>::new();
        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_table)(&table).unwrap();
        let any = (info.deserialize_table)(&bytes).unwrap();
        let recovered = any.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(recovered.len(), 0);
        assert_eq!(recovered.next_id(), 1);
    }

    #[test]
    fn table_roundtrip_preserves_next_id() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();

        let mut table = Table::<TestUser>::new();
        table.insert(TestUser { name: "A".into(), age: 1 }).unwrap();
        table.insert(TestUser { name: "B".into(), age: 2 }).unwrap();
        table.delete(2).unwrap(); // next_id stays at 3

        let info = reg.get("users").unwrap();
        let bytes = (info.serialize_table)(&table).unwrap();
        let any = (info.deserialize_table)(&bytes).unwrap();
        let recovered = any.downcast_ref::<Table<TestUser>>().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered.next_id(), 3);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let reg = TableRegistry::default();
        assert!(reg.get("nope").is_none());
    }

    #[test]
    fn deserialize_record_bad_bytes_errors() {
        let mut reg = TableRegistry::default();
        reg.register::<TestUser>("users").unwrap();
        let info = reg.get("users").unwrap();
        let result = (info.deserialize_record)(&[0xFF, 0xFF, 0xFF]);
        assert!(matches!(result, Err(Error::Persistence(_))));
    }
}
