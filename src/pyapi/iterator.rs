use pyo3::prelude::*;

/// Source of batches for a [`PyBatchIterator`]: an eagerly collected Vec
/// (non-native formats / explicit collect) or a live DataFusion stream.
pub enum PyBatchSource {
    Eager(std::vec::IntoIter<arrow::array::RecordBatch>),
    Lazy(datafusion::physical_plan::SendableRecordBatchStream),
}

/// Synchronous Python iterator yielding RecordBatch streams from DataFusion SQL execution
#[pyclass]
pub struct PyBatchIterator {
    pub source: std::sync::Mutex<PyBatchSource>,
    pub total_batches: usize,
    pub total_rows: usize,
}

impl PyBatchIterator {
    pub fn new(batches: Vec<arrow::array::RecordBatch>) -> Self {
        let total_batches = batches.len();
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            source: std::sync::Mutex::new(PyBatchSource::Eager(batches.into_iter())),
            total_batches,
            total_rows,
        }
    }

    pub fn from_stream(stream: datafusion::physical_plan::SendableRecordBatchStream) -> Self {
        // Row counts are unknown until the stream is consumed.
        Self {
            source: std::sync::Mutex::new(PyBatchSource::Lazy(stream)),
            total_batches: 0,
            total_rows: 0,
        }
    }
}

#[pymethods]
impl PyBatchIterator {
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Human-readable summary shown in notebook cells
    pub fn __repr__(&self) -> String {
        format!(
            "PyBatchIterator(batches={}, rows={})",
            self.total_batches, self.total_rows
        )
    }

    pub fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        use arrow::pyarrow::ToPyArrow;
        use futures::StreamExt;
        let mut guard = self
            .source
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        match &mut *guard {
            PyBatchSource::Eager(iter) => {
                if let Some(batch) = iter.next() {
                    let py_batch = batch.to_pyarrow(py)?;
                    Ok(Some(py_batch))
                } else {
                    Ok(None)
                }
            }
            PyBatchSource::Lazy(stream) => {
                match crate::engine::memory::global_runtime().block_on(stream.next()) {
                    Some(Ok(batch)) => {
                        let py_batch = batch.to_pyarrow(py)?;
                        Ok(Some(py_batch))
                    }
                    Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                    None => Ok(None),
                }
            }
        }
    }

    /// Conversion of stream into PyArrow Table
    pub fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use arrow::pyarrow::ToPyArrow;
        use futures::StreamExt;
        let mut guard = self
            .source
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let pyarrow = py.import("pyarrow")?;
        let mut py_batches = Vec::new();
        match &mut *guard {
            PyBatchSource::Eager(iter) => {
                for batch in iter.by_ref() {
                    py_batches.push(batch.to_pyarrow(py)?);
                }
            }
            PyBatchSource::Lazy(stream) => loop {
                match crate::engine::memory::global_runtime().block_on(stream.next()) {
                    Some(Ok(batch)) => py_batches.push(batch.to_pyarrow(py)?),
                    Some(Err(e)) => {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                    }
                    None => break,
                }
            },
        }
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (py_batches,))?;
        Ok(table)
    }
}