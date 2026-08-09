//! On-ramp helpers: a small pure-Rust "recommendation" surface that pairs with
//! the Rust-side budget work (see `memory.rs`) and surfaces tuning guidance to
//! callers. Pure advice — no execution paths are touched.

use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// Advise a sensible batch size given how many files/streams a job will
    /// process in parallel. Ties directly into `memory::budget_batch_rows`.
    pub fn recommend_batch_size(&self, parallel_streams: usize) -> usize {
        crate::engine::memory::budget_batch_rows(parallel_streams)
    }
}
