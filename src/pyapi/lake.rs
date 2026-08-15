use pyo3::prelude::*;

use super::default_engine;

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

#[pyfunction]
#[pyo3(signature = (src_dir, dst_dir, auto_normalize=None))]
fn ingest(
    py: Python<'_>,
    src_dir: &str,
    dst_dir: &str,
    auto_normalize: Option<bool>,
) -> PyResult<(usize, usize)> {
    default_engine().ingest(py, src_dir, dst_dir, auto_normalize)
}

#[pymodule]
pub fn lake(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process_and_write_lake, m)?)?;
    m.add_function(wrap_pyfunction!(generate_gold_table, m)?)?;
    m.add_function(wrap_pyfunction!(split_file, m)?)?;
    m.add_function(wrap_pyfunction!(ingest, m)?)?;
    Ok(())
}
