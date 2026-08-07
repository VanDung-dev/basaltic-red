use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::engine::{MatrixEngine, PyBatchIterator};

fn default_engine() -> MatrixEngine {
    MatrixEngine::new(1, 9, 0.01, 100.0)
}

// ---- read ----

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

// ---- sql ----

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

// ---- filter ----

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

// ---- lake ----

#[pyfunction]
fn process_and_write_lake(
    py: Python<'_>,
    input_dir: &str,
    clean_output_dir: &str,
    trash_output_dir: &str,
    partition_filter: Option<&str>,
    batch_size: usize,
) -> PyResult<(usize, usize, usize, usize)> {
    default_engine().process_and_write_lake(
        py,
        input_dir,
        clean_output_dir,
        trash_output_dir,
        partition_filter,
        batch_size,
    )
}

#[pyfunction]
fn generate_gold_table(
    py: Python<'_>,
    input_dir: &str,
    gold_output_dir: &str,
    table_version: &str,
    partition_filter: Option<&str>,
    batch_size: usize,
) -> PyResult<(usize, usize, String)> {
    default_engine().generate_gold_table(
        py,
        input_dir,
        gold_output_dir,
        table_version,
        partition_filter,
        batch_size,
    )
}

#[pyfunction]
fn split_file(
    py: Python<'_>,
    file_path: &str,
    max_rows_per_file: usize,
    output_dir: &str,
    format: &str,
) -> PyResult<usize> {
    default_engine().split_file(py, file_path, max_rows_per_file, output_dir, format)
}

#[pymodule]
pub fn lake(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process_and_write_lake, m)?)?;
    m.add_function(wrap_pyfunction!(generate_gold_table, m)?)?;
    m.add_function(wrap_pyfunction!(split_file, m)?)?;
    Ok(())
}

// ---- dictionary ----

#[pyfunction]
fn export_data_dictionary_md(
    py: Python<'_>,
    target_path: &str,
    output_path: &str,
) -> PyResult<String> {
    default_engine().export_data_dictionary_md(py, target_path, output_path)
}

#[pymodule]
pub fn dictionary(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(export_data_dictionary_md, m)?)?;
    Ok(())
}

// ---- graph ----

#[pyfunction]
fn generate_er_graph(py: Python<'_>, path: &str, output_path: Option<&str>) -> PyResult<String> {
    default_engine().generate_er_graph_py(py, path, output_path)
}

#[pymodule]
pub fn graph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_er_graph, m)?)?;
    Ok(())
}
