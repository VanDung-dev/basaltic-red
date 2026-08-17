pub mod dictionary;
pub mod engine;
pub mod filter;
pub mod formats;
pub mod graph;
pub mod iterator;
pub mod lake;
pub mod read;
pub mod sql;

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::engine::MatrixEngine;
use crate::error::BazanError;

/// Single point mapping `BazanError` → the matching Python exception, replacing
/// the copy-pasted `map_err(|e| PyXxx::new_err(e.to_string()))` glue.
pub fn bazan_to_pyerr(e: BazanError) -> PyErr {
    match &e {
        BazanError::UnsupportedFormat(_) => PyValueError::new_err(e.to_string()),
        BazanError::DataFusion(_) => PyRuntimeError::new_err(e.to_string()),
        _ => PyIOError::new_err(e.to_string()),
    }
}

static DEFAULT_ENGINE: std::sync::OnceLock<MatrixEngine> = std::sync::OnceLock::new();

/// Shared default engine behind the namespaced `br.read.*` / `br.sql.*` / ... API.
/// Constructed once, so thresholds never drift between sub-commands.
pub fn default_engine() -> &'static MatrixEngine {
    DEFAULT_ENGINE.get_or_init(|| MatrixEngine::new(1, 9, 0.01, 100.0))
}