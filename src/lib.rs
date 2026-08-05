use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub mod cli;
pub mod engine;
pub mod error;
pub mod filter;
pub mod utils;

use engine::MatrixEngine;

/// Read a `.bazan` container manifest, returning {version, entries: [{path, offset, length, format, num_rows}, ...]}.
#[pyfunction]
fn read_bazan_manifest(py: Python, bazan_path: &str) -> PyResult<Py<PyDict>> {
    use engine::container::read_bazan_manifest as read_manifest;

    let manifest = read_manifest(std::path::Path::new(bazan_path))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    let dict = PyDict::new(py);
    dict.set_item("version", manifest.version)?;

    let entries = PyList::empty(py);
    for e in &manifest.entries {
        let ed = PyDict::new(py);
        ed.set_item("path", &e.path)?;
        ed.set_item("offset", e.offset)?;
        ed.set_item("length", e.length)?;
        ed.set_item("format", &e.format)?;
        ed.set_item("num_rows", e.num_rows)?;
        entries.append(ed)?;
    }
    dict.set_item("entries", entries)?;

    Ok(dict.unbind())
}

#[pymodule]
fn basaltic_red(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MatrixEngine>()?;
    m.add_class::<engine::PyBatchIterator>()?;
    m.add_function(wrap_pyfunction!(read_bazan_manifest, m)?)?;
    Ok(())
}
