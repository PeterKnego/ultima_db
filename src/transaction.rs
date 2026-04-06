// ReadTx and WriteTx are defined in store.rs (alongside Snapshot and Store)
// to avoid a circular module dependency. They are re-exported here so that
// users can import them from a semantically clear location.
pub use crate::store::{ReadTx, Readable, TableWriter, WriteTx};
