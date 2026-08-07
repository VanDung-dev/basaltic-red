use arrow::array::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::Py;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use crate::engine::MatrixEngine;
use crate::error::BazanError;

impl MatrixEngine {
    /// Fast Sample Preview Extraction for DuckDB 1.4.5 In-Memory Interactive Preview
    pub fn preview_parquet_sample<'py>(
        &self,
        py: Python<'py>,
        file_path: &str,
        limit_rows: usize,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let limit_rows = crate::engine::formats::clamp_batch_size(limit_rows);
        let path = file_path.to_string();
        let ext = std::path::Path::new(&path)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        let (clean_b, trash_b) = py
            .detach(|| -> Result<(RecordBatch, RecordBatch), BazanError> {
                let handler = crate::engine::formats::handler_for(&ext)
                    .ok_or_else(|| BazanError::UnsupportedFormat(ext.clone()))?;
                let mut source = handler.open(&path, limit_rows)?;
                if let Some(batch_res) = source.batches.next() {
                    let batch = batch_res?;
                    let rows = batch.num_rows();
                    Ok(self.filter_batch_native(&batch, rows))
                } else {
                    Err(BazanError::Message(format!(".{} file is empty", ext)))
                }
            })
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok((
            clean_b.to_pyarrow(py)?.into(),
            trash_b.to_pyarrow(py)?.into(),
        ))
    }

    /// NATIVE Data Dictionary Generator
    pub fn export_data_dictionary_md_inner(
        &self,
        py: Python<'_>,
        target_path: &str,
        output_md_path: &str,
    ) -> PyResult<String> {
        let t_path = target_path.to_string();
        let out_md = output_md_path.to_string();

        let res = py.detach(|| -> Result<String, BazanError> {
            let path_obj = std::path::Path::new(&t_path);
            let sample_file_path = if path_obj.is_dir() {
                let discovered = crate::utils::discover_data_files(path_obj, None)?;
                if discovered.is_empty() {
                    return Err(BazanError::Message(format!(
                        "No supported data files found in directory: {}",
                        t_path
                    )));
                }
                discovered[0].clone()
            } else if path_obj.exists() {
                path_obj.to_path_buf()
            } else {
                return Err(BazanError::Message(format!(
                    "Target path does not exist: {}",
                    t_path
                )));
            };

            let ext = sample_file_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_lowercase();
            let schema = if ext == "csv" {
                let file = File::open(&sample_file_path)?;
                let (schema, _) = arrow_csv::reader::Format::default()
                    .with_header(true)
                    .infer_schema(file, Some(100))?;
                Arc::new(schema)
            } else if ext == "tsv" {
                let file = File::open(&sample_file_path)?;
                let (schema, _) = arrow_csv::reader::Format::default()
                    .with_delimiter(b'\t')
                    .with_header(true)
                    .infer_schema(file, Some(100))?;
                Arc::new(schema)
            } else if ext == "psv" {
                let file = File::open(&sample_file_path)?;
                let (schema, _) = arrow_csv::reader::Format::default()
                    .with_delimiter(b'|')
                    .with_header(true)
                    .infer_schema(file, Some(100))?;
                Arc::new(schema)
            } else if ext == "txt" {
                let file = File::open(&sample_file_path)?;
                let (schema, _) = arrow_csv::reader::Format::default()
                    .with_delimiter(b';')
                    .with_header(true)
                    .infer_schema(file, Some(100))?;
                Arc::new(schema)
            } else if ext == "feather" || ext == "arrow" || ext == "ipc" {
                let file = File::open(&sample_file_path)?;
                let reader = arrow_ipc::reader::FileReader::try_new(file, None)?;
                reader.schema().clone()
            } else if ext == "parquet" || ext == "pq" {
                let file = File::open(&sample_file_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
                builder.schema().clone()
            } else {
                let file = File::open(&sample_file_path)?;
                let mut buf_reader = BufReader::new(file);
                let (schema, _) =
                    arrow_json::reader::infer_json_schema(&mut buf_reader, Some(100))?;
                Arc::new(schema)
            };

            let mut md_lines = Vec::new();
            md_lines.push("| STT | Column Name | Data Type | Nullable | Description |".to_string());
            md_lines.push("|---|---|---|---|---|".to_string());

            for (idx, field) in schema.fields().iter().enumerate() {
                let col_name = field.name();
                let data_type = field.data_type().to_string();
                let nullable = if field.is_nullable() { "Yes" } else { "No" };

                md_lines.push(format!(
                    "| **{}** | `{}` | `{}` | {} | |",
                    idx + 1,
                    col_name,
                    data_type,
                    nullable
                ));
            }

            let content = md_lines.join("\n");
            std::fs::write(&out_md, content)?;

            Ok(out_md)
        });

        match res {
            Ok(path) => Ok(path),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
