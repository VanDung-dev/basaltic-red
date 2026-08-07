use pyo3::prelude::*;

pub mod cli;
pub mod engine;
pub mod error;
pub mod filter;
pub mod utils;

use engine::MatrixEngine;

#[pymodule]
fn basaltic_red(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MatrixEngine>()?;
    m.add_class::<engine::PyBatchIterator>()?;
    Ok(())
}
