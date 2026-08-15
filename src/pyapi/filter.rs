use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use super::default_engine;

#[pyfunction]
fn process_batch<'py>(
    py: Python<'py>,
    batch: &Bound<'py, PyAny>,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    default_engine().process_batch(py, batch)
}

#[pyfunction]
fn process_file(
    py: Python<'_>,
    file_path: &str,
    batch_size: usize,
) -> PyResult<(usize, usize, usize)> {
    default_engine().process_file(py, file_path, batch_size)
}

#[pyfunction]
fn filter_matrix<'py>(
    py: Python<'py>,
    file_path: &str,
    rules: Vec<String>,
) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
    default_engine().filter_matrix(py, file_path, rules)
}

#[pyfunction]
#[pyo3(signature = (path_pattern, rules, partition_filter=None, num_threads=None))]
fn filter_files_parallel<'py>(
    py: Python<'py>,
    path_pattern: &str,
    rules: Vec<String>,
    partition_filter: Option<&str>,
    num_threads: Option<usize>,
) -> PyResult<Py<PyDict>> {
    default_engine().filter_files_parallel(
        py,
        path_pattern,
        rules,
        partition_filter,
        num_threads,
    )
}

#[pymodule]
pub fn filter(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process_batch, m)?)?;
    m.add_function(wrap_pyfunction!(process_file, m)?)?;
    m.add_function(wrap_pyfunction!(filter_matrix, m)?)?;
    m.add_function(wrap_pyfunction!(filter_files_parallel, m)?)?;
    Ok(())
}
