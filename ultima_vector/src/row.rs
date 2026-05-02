// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Row types stored in UltimaDB tables.
//!
//! `VectorRow<Meta>` is the user-facing row in the data table. `HnswState`
//! is the per-node graph state denormalized into the row. `EntryPoint` is
//! the singleton row of the auxiliary entry-point table.

#[cfg(feature = "persistence")]
use serde::{Deserialize, Serialize};

/// User-facing row stored in `Table<VectorRow<Meta>>`.
///
/// Holds the embedding, the user's metadata, and the HNSW node state. The
/// HNSW state is conceptually private — users should mutate rows only via
/// `VectorCollection`'s API, never by editing `hnsw` directly.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "persistence", derive(Serialize, Deserialize))]
pub struct VectorRow<Meta> {
    pub embedding: Vec<f32>,
    pub meta: Meta,
    pub hnsw: HnswState,
}

/// Per-node HNSW graph state. `layers[0]` is the base layer; `layers[i]`
/// for `i > 0` is the corresponding upper layer. A node at `level = L`
/// has `L + 1` layer entries (layers `0..=L`).
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "persistence", derive(Serialize, Deserialize))]
pub struct HnswState {
    level: u8,
    tombstoned: bool,
    layers: Vec<Vec<u64>>,
}

impl HnswState {
    /// New state for a node at the given top level, with empty adjacency lists.
    pub fn empty(level: u8) -> Self {
        let layers = vec![Vec::new(); usize::from(level) + 1];
        Self { level, tombstoned: false, layers }
    }

    pub fn level(&self) -> u8 {
        self.level
    }

    pub fn is_tombstoned(&self) -> bool {
        self.tombstoned
    }

    pub fn tombstone(&mut self) {
        self.tombstoned = true;
    }

    /// Number of layer entries (`level + 1`).
    pub fn layers_len(&self) -> usize {
        self.layers.len()
    }

    /// Adjacency at the given layer; empty slice if the layer is above this
    /// node's level.
    pub fn neighbors(&self, layer: u8) -> &[u64] {
        self.layers
            .get(usize::from(layer))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Replace adjacency at the given layer. Panics if `layer` is above the
    /// node's `level` (callers must respect node level invariants).
    pub fn set_neighbors(&mut self, layer: u8, neighbors: Vec<u64>) {
        let idx = usize::from(layer);
        assert!(idx < self.layers.len(), "layer {layer} above node level {}", self.level);
        self.layers[idx] = neighbors;
    }
}

/// Singleton row in the entry-point auxiliary table. Holds the node id of
/// the HNSW entry point (the node sitting at the highest layer of the
/// graph) and the current global maximum level.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "persistence", derive(Serialize, Deserialize))]
pub struct EntryPoint {
    pub node_id: Option<u64>,
    pub max_level: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_state_at_level_has_layer_count() {
        let s = HnswState::empty(0);
        assert_eq!(s.level(), 0);
        assert_eq!(s.layers_len(), 1);
        assert!(s.neighbors(0).is_empty());

        let s = HnswState::empty(3);
        assert_eq!(s.level(), 3);
        assert_eq!(s.layers_len(), 4);
        for layer in 0..=3 {
            assert!(s.neighbors(layer).is_empty());
        }
    }

    #[test]
    fn empty_state_is_not_tombstoned() {
        assert!(!HnswState::empty(2).is_tombstoned());
    }

    #[test]
    fn tombstone_sets_flag() {
        let mut s = HnswState::empty(0);
        s.tombstone();
        assert!(s.is_tombstoned());
    }

    #[test]
    fn set_neighbors_replaces_layer() {
        let mut s = HnswState::empty(2);
        s.set_neighbors(0, vec![1, 2, 3]);
        s.set_neighbors(1, vec![4, 5]);
        s.set_neighbors(2, vec![]);
        assert_eq!(s.neighbors(0), &[1, 2, 3]);
        assert_eq!(s.neighbors(1), &[4, 5]);
        assert_eq!(s.neighbors(2), &[] as &[u64]);
    }

    #[test]
    fn neighbors_above_level_is_empty() {
        let s = HnswState::empty(0);
        assert!(s.neighbors(5).is_empty(), "out-of-bounds layer returns empty");
    }

    #[test]
    fn vector_row_roundtrip_clone() {
        let row = VectorRow {
            embedding: vec![0.1, 0.2, 0.3],
            meta: 42u64,
            hnsw: HnswState::empty(1),
        };
        let cloned = row.clone();
        assert_eq!(row.embedding, cloned.embedding);
        assert_eq!(row.meta, cloned.meta);
        assert_eq!(row.hnsw.level(), cloned.hnsw.level());
    }

    #[test]
    fn entry_point_default_is_empty() {
        let ep = EntryPoint::default();
        assert!(ep.node_id.is_none());
        assert_eq!(ep.max_level, 0);
    }
}
