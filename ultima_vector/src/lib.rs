//! `ultima_vector` — HNSW vector search built on UltimaDB.

pub mod collection;
pub mod distance;
pub mod error;
pub mod filter;
pub mod hnsw;
pub mod row;

pub use collection::VectorCollection;
pub use distance::{Cosine, Distance, DotProduct, L2};
pub use error::{Error, Result};
pub use hnsw::params::HnswParams;
pub use row::VectorRow;
