// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ultima_vector` — HNSW vector search built on UltimaDB.

pub mod collection;
pub mod distance;
pub mod error;
pub mod filter;
pub mod hnsw;
pub mod row;

pub use collection::VectorCollection;
pub use distance::{
    Cosine, CosineNormalized, Distance, DotProduct, L2, normalize_in_place, normalize_many,
};
pub use error::{Error, Result};
pub use hnsw::params::HnswParams;
pub use row::VectorRow;
