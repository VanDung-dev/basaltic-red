use pyo3::prelude::*;

pub mod engine;
pub mod error;
pub mod filter;
pub mod pyapi;
pub mod utils;

#[pymodule]
mod basaltic_red {
    #[pymodule_export]
    use crate::engine::{MatrixEngine, PyBatchIterator};

    #[pymodule_export]
    use crate::pyapi::{dictionary, filter, formats, graph, lake, read, sql};
}
