use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (ext, delimiter, has_header=true))]
fn register_delimited(
    _py: Python<'_>,
    ext: &str,
    delimiter: &str,
    has_header: bool,
) -> PyResult<()> {
    let delim_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
    let handler = std::sync::Arc::new(crate::engine::formats::DelimitedFormatHandler::new(
        delim_byte,
        has_header,
    ));
    crate::engine::formats::register_format(ext, handler);
    Ok(())
}

#[pyfunction]
fn unregister_format(_py: Python<'_>, ext: &str) -> PyResult<bool> {
    Ok(crate::engine::formats::unregister_format(ext))
}

#[pyfunction]
fn list_formats(_py: Python<'_>) -> PyResult<Vec<String>> {
    Ok(crate::engine::formats::list_supported_formats())
}

#[pymodule]
pub fn formats(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(register_delimited, m)?)?;
    m.add_function(wrap_pyfunction!(unregister_format, m)?)?;
    m.add_function(wrap_pyfunction!(list_formats, m)?)?;
    Ok(())
}
