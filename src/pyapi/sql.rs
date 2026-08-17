use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::default_engine;
use crate::pyapi::iterator::PyBatchIterator;

#[pyfunction]
fn execute_sql<'py>(py: Python<'py>, query: &str) -> PyResult<Bound<'py, PyAny>> {
    default_engine().execute_sql_py(py, query)
}

#[pyfunction]
fn execute_sql_stream<'py>(py: Python<'py>, query: &str) -> PyResult<PyBatchIterator> {
    default_engine().execute_sql_stream_py(py, query)
}

#[pymodule]
pub fn sql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(execute_sql, m)?)?;
    m.add_function(wrap_pyfunction!(execute_sql_stream, m)?)?;
    Ok(())
}
