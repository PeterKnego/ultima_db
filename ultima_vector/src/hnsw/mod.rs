//! HNSW algorithm building blocks.

pub mod insert;
pub mod level;
pub mod params;
pub mod search;

use crate::row::VectorRow;
use ultima_db::{Record, TableReader, TableWriter};

/// Read-only access to vector rows. Implemented for both `TableReader`
/// and `TableWriter` so the same algorithm code drives both query and
/// build paths without duplication.
pub trait NodeAccess<Meta: Record> {
    fn get_row(&self, id: u64) -> Option<&VectorRow<Meta>>;
}

impl<Meta: Record> NodeAccess<Meta> for TableReader<'_, VectorRow<Meta>> {
    fn get_row(&self, id: u64) -> Option<&VectorRow<Meta>> {
        self.get(id)
    }
}

impl<Meta: Record> NodeAccess<Meta> for TableWriter<'_, VectorRow<Meta>> {
    fn get_row(&self, id: u64) -> Option<&VectorRow<Meta>> {
        self.get(id)
    }
}

/// `(id, distance)` pair used in HNSW heaps. `Ord` is by distance; ties broken by id
/// to make ordering total under f32 equality.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Candidate {
    pub id: u64,
    pub dist: f32,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
