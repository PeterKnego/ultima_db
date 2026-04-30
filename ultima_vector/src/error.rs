//! Error type for `ultima_vector`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("vector dim mismatch: expected {expected}, got {got}")]
    DimMismatch { expected: usize, got: usize },

    #[error("node id {0} not found")]
    NodeNotFound(u64),

    #[error("ultima_db error: {0}")]
    Storage(#[from] ultima_db::Error),

    #[error("invalid params: {0}")]
    InvalidParams(&'static str),

    #[error("invalid HnswState at id {id}: level={level} but layers.len()={layers}")]
    InvalidHnswState { id: u64, level: u8, layers: usize },
}

pub type Result<T> = std::result::Result<T, Error>;
