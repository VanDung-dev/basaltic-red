pub mod dictionary;
pub mod filter;
pub mod formats;
pub mod graph;
pub mod lake;
pub mod read;
pub mod sql;

use crate::engine::MatrixEngine;

pub fn default_engine() -> MatrixEngine {
    MatrixEngine::new(1, 9, 0.01, 100.0)
}
