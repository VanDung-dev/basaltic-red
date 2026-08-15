use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::default_engine;

#[pyfunction]
fn slice_rows(py: Python<'_>, file_path: &str, offset: usize, limit: usize) -> PyResult<Py<PyAny>> {
    default_engine().slice_rows(py, file_path, offset, limit)
}

#[pyfunction]
fn slice_cols(
    py: Python<'_>,
    file_path: &str,
    selected_cols: Vec<String>,
    offset: usize,
    limit: usize,
) -> PyResult<Py<PyAny>> {
    default_engine().slice_cols(py, file_path, selected_cols, offset, limit)
}

#[pyfunction]
fn preview_sample(
    py: Python<'_>,
    file_path: &str,
    limit_rows: usize,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    default_engine().preview_sample(py, file_path, limit_rows)
}

#[pymodule]
pub fn read(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(slice_rows, m)?)?;
    m.add_function(wrap_pyfunction!(slice_cols, m)?)?;
    m.add_function(wrap_pyfunction!(preview_sample, m)?)?;
    Ok(())
}
