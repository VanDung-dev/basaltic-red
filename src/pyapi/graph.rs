use pyo3::prelude::*;

use super::default_engine;

#[pyfunction]
fn generate_er_graph(py: Python<'_>, path: &str, output_path: Option<&str>) -> PyResult<String> {
    default_engine().generate_er_graph_py(py, path, output_path)
}

#[pymodule]
pub fn graph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_er_graph, m)?)?;
    Ok(())
}
