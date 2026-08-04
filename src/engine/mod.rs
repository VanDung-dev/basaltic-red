use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3::Py;

use crate::error::BazanError;

pub mod container;
pub mod dictionary;
pub mod dynamic_filter;
pub mod filter;
pub mod formats;
pub mod graph;
pub mod parallel_filter;
pub mod partition;
pub mod slice;
pub mod splitter;
pub mod sql;

pub use formats::*;

/// Core SIMD Matrix Engine supporting Audit Error Bitmasking for Matrix Trash & Parquet Streaming
#[pyclass]
pub struct MatrixEngine {
    pub min_passenger: i64,
    pub max_passenger: i64,
    pub min_fare: f64,
    pub max_speed_mph: f64,
}

impl MatrixEngine {
    /// Pure-Rust constructor (Python `MatrixEngine(...)` delegates here via `#[new]`).
    pub fn new(min_passenger: i64, max_passenger: i64, min_fare: f64, max_speed_mph: f64) -> Self {
        Self {
            min_passenger,
            max_passenger,
            min_fare,
            max_speed_mph,
        }
    }
}

#[pymethods]
impl MatrixEngine {
    #[new]
    #[pyo3(signature = (min_passenger=1, max_passenger=9, min_fare=0.01, max_speed_mph=100.0))]
    pub fn py_new(
        min_passenger: i64,
        max_passenger: i64,
        min_fare: f64,
        max_speed_mph: f64,
    ) -> Self {
        Self::new(min_passenger, max_passenger, min_fare, max_speed_mph)
    }

    /// Pack a directory into a .bazan container file
    pub fn pack_directory(&self, input_dir: &str, output_file: &str) -> PyResult<(usize, u64)> {
        let input_path = std::path::Path::new(input_dir);
        let output_path = std::path::Path::new(output_file);
        self.pack_directory_to_bazan(input_path, output_path)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Execute SQL query directly and return PyArrow Table
    #[pyo3(name = "execute_sql")]
    pub fn execute_sql_py<'py>(&self, py: Python<'py>, query: &str) -> PyResult<Bound<'py, PyAny>> {
        use arrow::pyarrow::ToPyArrow;
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let batch = rt
            .block_on(self.execute_sql(query))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        batch.to_pyarrow(py)
    }

    /// Filters a PyArrow RecordBatch into Clean RecordBatch and Trash RecordBatch (with Audit Error Bitmask)
    pub fn process_batch<'py>(
        &self,
        py: Python<'py>,
        batch_obj: &Bound<'py, PyAny>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        use arrow::array::RecordBatch;
        use arrow::pyarrow::{FromPyArrow, ToPyArrow};

        let record_batch = RecordBatch::from_pyarrow_bound(batch_obj)?;
        let total_rows = record_batch.num_rows();

        let (clean_b, trash_b) = py.detach(|| self.filter_batch_native(&record_batch, total_rows));

        Ok((
            clean_b.to_pyarrow(py)?.into(),
            trash_b.to_pyarrow(py)?.into(),
        ))
    }

    /// Unified Smart Reader: Automatically detects file extension
    pub fn process_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path_obj = std::path::Path::new(file_path);
        let ext = path_obj
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        let handler = formats::handler_for(&ext).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Unsupported file format: '.{}'. Supported formats: csv, tsv, psv, txt, json, jsonl, ndjson, parquet, pq, feather, arrow, ipc, avro, xlsx, orc, msgpack",
                ext
            ))
        })?;

        let stats = py.detach(|| handler.process_file(self, file_path, batch_size));

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// Enterprise Multi-File Partition Handler & Async Parquet Writer
    pub fn process_and_write_lake(
        &self,
        py: Python<'_>,
        input_dir: &str,
        clean_output_dir: &str,
        trash_output_dir: &str,
        partition_filter: Option<&str>,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize, usize)> {
        let in_dir = input_dir.to_string();
        let clean_dir = clean_output_dir.to_string();
        let trash_dir = trash_output_dir.to_string();
        let filter_str = partition_filter.map(|s| s.to_string());

        let stats = py.detach(|| -> Result<(usize, usize, usize, usize), BazanError> {
            self.process_and_write_lake_native(
                &in_dir,
                &clean_dir,
                &trash_dir,
                filter_str.as_deref(),
                batch_size,
            )
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// Read a specific row range zero-copy as PyArrow Table
    pub fn slice_rows<'py>(
        &self,
        py: Python<'py>,
        file_path: &str,
        offset: usize,
        limit: usize,
    ) -> PyResult<Py<PyAny>> {
        use arrow::pyarrow::ToPyArrow;
        let path = file_path.to_string();
        let batch = py
            .detach(|| self.slice_rows_native(&path, offset, limit))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(batch.to_pyarrow(py)?.into())
    }

    /// Read selected columns & row range zero-copy as PyArrow Table
    pub fn slice_cols<'py>(
        &self,
        py: Python<'py>,
        file_path: &str,
        selected_cols: Vec<String>,
        offset: usize,
        limit: usize,
    ) -> PyResult<Py<PyAny>> {
        use arrow::pyarrow::ToPyArrow;
        let path = file_path.to_string();
        let batch = py
            .detach(|| self.slice_cols_native(&path, &selected_cols, offset, limit))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(batch.to_pyarrow(py)?.into())
    }

    /// Split large matrix file into smaller part files
    pub fn split_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        max_rows_per_file: usize,
        output_dir: &str,
        format: &str,
    ) -> PyResult<usize> {
        let path = file_path.to_string();
        let out_dir = output_dir.to_string();
        let fmt = format.to_string();
        py.detach(|| self.split_file_native(&path, max_rows_per_file, &out_dir, &fmt))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Multi-threaded parallel filter over a directory / glob / .bazan container.
    /// Returns a dict: {total_files, pruned_dirs, total_rows, clean_rows, trash_rows}.
    #[pyo3(signature = (path_pattern, rules, partition_filter=None, num_threads=None))]
    pub fn filter_files_parallel<'py>(
        &self,
        py: Python<'py>,
        path_pattern: &str,
        rules: Vec<String>,
        partition_filter: Option<&str>,
        num_threads: Option<usize>,
    ) -> PyResult<Py<PyDict>> {
        use crate::engine::dynamic_filter::FilterRule;
        use crate::engine::parallel_filter::ParallelFilterSummary;

        let path = path_pattern.to_string();
        let filter_str = partition_filter.map(|s| s.to_string());
        let parsed_rules: Vec<FilterRule> = rules
            .iter()
            .map(|r| FilterRule::parse(r))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let summary: ParallelFilterSummary = py
            .detach(|| -> Result<ParallelFilterSummary, BazanError> {
                self.filter_files_parallel_native(
                    &path,
                    &parsed_rules,
                    filter_str.as_deref(),
                    num_threads,
                )
            })
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let dict = PyDict::new(py);
        dict.set_item("total_files", summary.total_files)?;
        dict.set_item("pruned_dirs", summary.pruned_dirs)?;
        dict.set_item("total_rows", summary.total_rows)?;
        dict.set_item("clean_rows", summary.clean_rows)?;
        dict.set_item("trash_rows", summary.trash_rows)?;
        Ok(dict.unbind())
    }

    /// Generate Mermaid ER Diagram from matrix schemas
    pub fn generate_er_graph_py(
        &self,
        py: Python<'_>,
        path: &str,
        output_path: Option<&str>,
    ) -> PyResult<String> {
        let input_path = path.to_string();
        let out_path = output_path.map(|s| s.to_string());
        py.detach(|| self.generate_er_graph(&input_path, out_path.as_deref()))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Dynamic Column Rules Filter: Evaluates rules and returns (Clean PyArrow Table, Trash PyArrow Table)
    pub fn filter_matrix<'py>(
        &self,
        py: Python<'py>,
        file_path: &str,
        rules: Vec<String>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        use crate::engine::dynamic_filter::FilterRule;
        use arrow::pyarrow::ToPyArrow;

        let path = file_path.to_string();
        let parsed_rules: Vec<FilterRule> = rules
            .iter()
            .map(|r| FilterRule::parse(r))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let (clean_b, trash_b) = py
            .detach(|| -> Result<_, BazanError> {
                let batch = self.slice_rows_native(&path, 0, usize::MAX)?;
                let res = self.filter_batch_dynamic(&batch, &parsed_rules)?;
                Ok(res)
            })
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok((
            clean_b.to_pyarrow(py)?.into(),
            trash_b.to_pyarrow(py)?.into(),
        ))
    }

    /// Clean Gold Table Generator & Versioning
    pub fn generate_gold_table(
        &self,
        py: Python<'_>,
        input_dir: &str,
        gold_output_dir: &str,
        table_version: &str,
        partition_filter: Option<&str>,
        batch_size: usize,
    ) -> PyResult<(usize, usize, String)> {
        let in_dir = input_dir.to_string();
        let gold_dir = gold_output_dir.to_string();
        let ver_str = table_version.to_string();
        let filter_str = partition_filter.map(|s| s.to_string());

        let res = py.detach(|| -> Result<(usize, usize, String), BazanError> {
            self.generate_gold_table_native(
                &in_dir,
                &gold_dir,
                &ver_str,
                filter_str.as_deref(),
                batch_size,
            )
        });

        match res {
            Ok(val) => Ok(val),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// Exposes preview_parquet_sample to Python
    pub fn preview_sample<'py>(
        &self,
        py: Python<'py>,
        file_path: &str,
        limit_rows: usize,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        self.preview_parquet_sample(py, file_path, limit_rows)
    }

    /// Exposes export_data_dictionary_md to Python
    pub fn export_data_dictionary_md(
        &self,
        py: Python<'_>,
        target_path: &str,
        output_path: &str,
    ) -> PyResult<String> {
        self.export_data_dictionary_md_inner(py, target_path, output_path)
    }
}
