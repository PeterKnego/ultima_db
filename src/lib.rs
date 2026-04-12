pub mod btree;
pub mod error;
#[cfg(feature = "fulltext")]
pub mod fulltext;
pub mod index;
pub mod persistence;
#[cfg(feature = "persistence")]
pub(crate) mod checkpoint;
#[cfg(feature = "persistence")]
pub(crate) mod registry;
pub mod metrics;
pub mod store;
#[cfg(feature = "persistence")]
pub mod wal;
pub mod table;
pub mod transaction;

pub use btree::BTree;
pub use error::{Error, Result};
#[cfg(feature = "fulltext")]
pub use fulltext::{FullTextIndex, SearchResult};
pub use index::{CustomIndex, IndexKind};
pub use persistence::{Durability, Persistence, Record};
pub use metrics::{IndexMetricsSnapshot, MetricsSnapshot, TableMetricsSnapshot};
pub use store::{Readable, Store, StoreConfig, WriterMode};
pub use table::{Table, TableDef, TableOpener};
pub use transaction::{ReadTx, TableWriter, WriteTx};
