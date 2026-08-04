use arrow::array::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3::Py;
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader};
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
        let path_obj = std::path::Path::new(&path);
        let ext = path_obj
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        let (clean_b, trash_b) = py
            .detach(|| -> Result<(RecordBatch, RecordBatch), BazanError> {
                if ext == "csv" {
                    let file = File::open(&path)?;
                    let (schema, _) = arrow_csv::reader::Format::default()
                        .with_header(true)
                        .infer_schema(file, Some(100))?;

                    let file_for_reader = File::open(&path)?;
                    let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
                        .with_header(true)
                        .with_batch_size(limit_rows)
                        .build(file_for_reader)?;

                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("CSV file is empty".to_string()))
                    }
                } else if ext == "tsv" {
                    // Read header to extract col names, force all columns as Utf8
                    let hdr_file = File::open(&path)?;
                    let mut hdr_buf = BufReader::new(hdr_file);
                    let mut hdr_line = String::new();
                    hdr_buf.read_line(&mut hdr_line)?;
                    let col_names: Vec<String> = hdr_line
                        .trim_end()
                        .split('\t')
                        .map(|s| s.to_string())
                        .collect();
                    let fields: Vec<Field> = col_names
                        .iter()
                        .map(|n| Field::new(n, DataType::Utf8, true))
                        .collect();
                    let schema = Arc::new(Schema::new(fields));

                    let null_regex = Regex::new(r"^\\N$")?;
                    let file_for_reader = File::open(&path)?;
                    let mut reader = arrow_csv::ReaderBuilder::new(schema)
                        .with_header(true)
                        .with_delimiter(b'\t')
                        .with_null_regex(null_regex)
                        .with_truncated_rows(true)
                        .with_batch_size(limit_rows)
                        .build(file_for_reader)?;

                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("TSV file is empty".to_string()))
                    }
                } else if ext == "ndjson" {
                    let file = File::open(&path)?;
                    let mut buf_reader = BufReader::new(file);
                    let schema = arrow_json::reader::infer_json_schema_from_iterator(
                        arrow_json::reader::ValueIter::new(&mut buf_reader, Some(100)),
                    )?;

                    let file_for_reader = File::open(&path)?;
                    let buf_reader_2 = BufReader::new(file_for_reader);
                    let mut reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                        .with_batch_size(limit_rows)
                        .build(buf_reader_2)?;

                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("NDJSON file is empty".to_string()))
                    }
                } else if ext == "json" || ext == "jsonl" {
                    let file = File::open(&path)?;
                    let mut buf_reader = BufReader::new(file);
                    let (schema, _) =
                        arrow_json::reader::infer_json_schema(&mut buf_reader, Some(100))?;

                    let file_for_reader = File::open(&path)?;
                    let buf_reader_2 = BufReader::new(file_for_reader);
                    let mut reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                        .with_batch_size(limit_rows)
                        .build(buf_reader_2)?;

                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("JSON file is empty".to_string()))
                    }
                } else if ext == "psv" {
                    let file = File::open(&path)?;
                    let (schema, _) = arrow_csv::reader::Format::default()
                        .with_delimiter(b'|')
                        .with_header(true)
                        .infer_schema(file, Some(100))?;
                    let file_for_reader = File::open(&path)?;
                    let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
                        .with_delimiter(b'|')
                        .with_header(true)
                        .with_batch_size(limit_rows)
                        .build(file_for_reader)?;
                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("PSV file is empty".to_string()))
                    }
                } else if ext == "txt" {
                    let file = File::open(&path)?;
                    let (schema, _) = arrow_csv::reader::Format::default()
                        .with_delimiter(b';')
                        .with_header(true)
                        .infer_schema(file, Some(100))?;
                    let file_for_reader = File::open(&path)?;
                    let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
                        .with_delimiter(b';')
                        .with_header(true)
                        .with_batch_size(limit_rows)
                        .build(file_for_reader)?;
                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("TXT file is empty".to_string()))
                    }
                } else if ext == "feather" || ext == "arrow" || ext == "ipc" {
                    let file = File::open(&path)?;
                    let mut reader = arrow_ipc::reader::FileReader::try_new(file, None)?;
                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("Feather file is empty".to_string()))
                    }
                } else if ext == "parquet" || ext == "pq" {
                    let file = File::open(&path)?;
                    let builder =
                        ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(limit_rows);
                    let mut reader = builder.build()?;

                    if let Some(batch_res) = reader.next() {
                        let batch = batch_res?;
                        let rows = batch.num_rows();
                        Ok(self.filter_batch_native(&batch, rows))
                    } else {
                        Err(BazanError::Message("Parquet file is empty".to_string()))
                    }
                } else {
                    Err(BazanError::UnsupportedFormat(ext))
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
