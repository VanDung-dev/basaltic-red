use pyo3::prelude::*;

pub mod csv_guard;
pub mod dynamic_filter;
pub mod filter;
pub mod formats;
pub mod graph;
pub mod ingest;
pub mod memory;
pub mod parallel_filter;
pub mod partition;
pub mod recommend;
pub mod slice;
pub mod splitter;
pub mod sql;

pub use formats::*;

/// Core SIMD Matrix Engine supporting Audit Error Bitmasking for Matrix Trash & Parquet Streaming
#[pyclass]
pub struct MatrixEngine {
    pub min_passenger: i64,
    pub max_passenger: i64,
    pub min_fare: f64,
    pub max_speed_mph: f64,
}

impl MatrixEngine {
    /// Pure-Rust constructor (Python `MatrixEngine(...)` delegates here via `#[new]`).
    pub fn new(min_passenger: i64, max_passenger: i64, min_fare: f64, max_speed_mph: f64) -> Self {
        Self {
            min_passenger,
            max_passenger,
            min_fare,
            max_speed_mph,
        }
    }
}