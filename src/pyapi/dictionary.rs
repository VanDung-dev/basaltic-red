use pyo3::prelude::*;

use super::default_engine;

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
