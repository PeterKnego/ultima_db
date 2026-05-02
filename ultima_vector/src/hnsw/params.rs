// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! HNSW tuning parameters supplied at collection creation time.

#[cfg(feature = "persistence")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(Serialize, Deserialize))]
pub struct HnswParams {
    /// Embedding dimensionality. Every inserted vector must have this length.
    pub dim: usize,
    /// Max neighbors per node at upper layers. Typical 16.
    pub m: usize,
    /// Max neighbors per node at the base layer. Typical 2 * m = 32.
    pub m_max0: usize,
    /// Candidate-set size during build (`efConstruction`). Typical 100..200.
    pub ef_construction: usize,
    /// Candidate-set size during search (`efSearch`). Typical 50..100.
    pub ef_search_default: usize,
    /// Hard cap on node levels. Levels are sampled from a geometric
    /// distribution with mean `1/ln(m)`; `max_level` truncates the tail.
    pub max_level: u8,
}

impl HnswParams {
    /// Reasonable defaults for the given embedding dimensionality.
    pub fn for_dim(dim: usize) -> Self {
        Self {
            dim,
            m: 16,
            m_max0: 32,
            ef_construction: 100,
            ef_search_default: 50,
            max_level: 16,
        }
    }
}
