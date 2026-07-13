// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ultima_vector` — HNSW approximate-nearest-neighbor search built on
//! UltimaDB's MVCC tables.
//!
//! A [`VectorCollection`] stores embeddings plus caller-defined metadata
//! rows and answers top-k similarity queries, optionally filtered on that
//! metadata. Distance kernels ([`Cosine`], [`L2`], [`DotProduct`], and the
//! opt-in [`CosineNormalized`] fast path) are SIMD-accelerated
//! (AVX-512 / AVX2 / NEON selected at runtime via `pulp`).
//!
//! Inputs are validated at the collection boundary: dimension mismatches
//! and non-finite values (NaN/±Inf) are rejected on insert, update,
//! search, and restore.
//!
//! ```rust
//! use ultima_db::{Store, StoreConfig};
//! use ultima_vector::{Cosine, HnswParams, VectorCollection};
//!
//! # fn main() -> Result<(), ultima_vector::Error> {
//! let store = Store::new(StoreConfig::default())?;
//! let coll: VectorCollection<String, Cosine> =
//!     VectorCollection::open(store, "docs", HnswParams::for_dim(3), Cosine)?;
//!
//! coll.upsert(vec![0.1, 0.2, 0.3], "doc-a".to_string())?;
//! coll.upsert(vec![0.9, 0.1, 0.0], "doc-b".to_string())?;
//!
//! let hits = coll.search(&[0.1, 0.2, 0.25], 1, None, None)?;
//! assert_eq!(hits[0].0, 1); // id of the closest match, "doc-a"
//! # Ok(()) }
//! ```
//!
//! The `persistence` feature persists collections through UltimaDB's WAL /
//! checkpoint machinery; bulk restores go through the MVCC-consistent
//! restore path (see `examples/bulk_restore.rs`).

#![warn(missing_docs)]

pub mod collection;
pub mod distance;
pub mod error;
pub mod filter;
pub mod hnsw;
pub mod row;
mod validate;

pub use collection::VectorCollection;
pub use distance::{
    Cosine, CosineNormalized, Distance, DotProduct, L2, normalize_in_place, normalize_many,
};
pub use error::{Error, Result};
pub use hnsw::params::HnswParams;
pub use row::VectorRow;
