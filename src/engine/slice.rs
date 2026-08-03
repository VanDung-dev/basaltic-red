use arrow::array::RecordBatch;
use std::fs::File;
use std::sync::Arc;
use anyhow::{anyhow, Result};

use crate::engine::MatrixEngine;

/// Unified 2 GB RAM Budget Max Batch Size: 2^20 = 1,048,576 rows per batch (~500MB-1.5GB RAM)
pub const DEFAULT_MAX_BATCH_SIZE: usize = 1 << 20;

impl MatrixEngine {
    /// Read a specific row range (offset..offset+limit) zero-copy from any supported format
    pub fn slice_rows_native(&self, file_path: &str, offset: usize, limit: usize) -> Result<RecordBatch> {
        let path = std::path::Path::new(file_path);
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
        let target_batch_size = offset.saturating_add(limit).min(DEFAULT_MAX_BATCH_SIZE);

        if ext == "parquet" || ext == "pq" {
            let file = File::open(file_path)?;
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.with_batch_size(target_batch_size).build()?;
            
            let mut accumulated_rows = 0;
            let mut matched_batches = Vec::new();

            for batch_res in reader {
                let batch = batch_res?;
                let b_len = batch.num_rows();

                if accumulated_rows + b_len > offset {
                    let start_in_batch = if accumulated_rows < offset { offset - accumulated_rows } else { 0 };
                    let len_in_batch = (limit - matched_batches.iter().map(|b: &RecordBatch| b.num_rows()).sum::<usize>()).min(b_len - start_in_batch);
                    
                    if len_in_batch > 0 {
                        matched_batches.push(batch.slice(start_in_batch, len_in_batch));
                    }
                }

                accumulated_rows += b_len;
                if matched_batches.iter().map(|b| b.num_rows()).sum::<usize>() >= limit {
                    break;
                }
            }

            if matched_batches.is_empty() {
                let schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(File::open(file_path)?)?
                    .schema()
                    .clone();
                return Ok(RecordBatch::new_empty(schema));
            }

            arrow::compute::concat_batches(&matched_batches[0].schema(), &matched_batches).map_err(|e| anyhow!(e))
        } else if ext == "csv" || ext == "tsv" || ext == "psv" || ext == "txt" {
            let delimiter = match ext.as_str() {
                "tsv" => b'\t',
                "psv" => b'|',
                "txt" => b';',
                _ => b',',
            };

            let file = File::open(file_path)?;
            let (schema, _) = arrow_csv::reader::Format::default()
                .with_header(true)
                .with_delimiter(delimiter)
                .infer_schema(file, Some(100))?;

            let file_reader = File::open(file_path)?;
            let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema.clone()))
                .with_header(true)
                .with_delimiter(delimiter)
                .with_batch_size(target_batch_size)
                .build(file_reader)?;

            let mut accumulated_rows = 0;
            let mut matched_batches = Vec::new();

            for batch_res in reader {
                let batch = batch_res?;
                let b_len = batch.num_rows();

                if accumulated_rows + b_len > offset {
                    let start_in_batch = if accumulated_rows < offset { offset - accumulated_rows } else { 0 };
                    let len_in_batch = (limit - matched_batches.iter().map(|b: &RecordBatch| b.num_rows()).sum::<usize>()).min(b_len - start_in_batch);

                    if len_in_batch > 0 {
                        matched_batches.push(batch.slice(start_in_batch, len_in_batch));
                    }
                }

                accumulated_rows += b_len;
                if matched_batches.iter().map(|b| b.num_rows()).sum::<usize>() >= limit {
                    break;
                }
            }

            if matched_batches.is_empty() {
                return Ok(RecordBatch::new_empty(Arc::new(schema)));
            }

            arrow::compute::concat_batches(&matched_batches[0].schema(), &matched_batches).map_err(|e| anyhow!(e))
        } else {
            Err(anyhow!("Format '.{}' slicing not supported yet", ext))
        }
    }

    /// Read selected columns & row range (Column Projection zero-copy)
    pub fn slice_cols_native(
        &self,
        file_path: &str,
        selected_cols: &[String],
        offset: usize,
        limit: usize,
    ) -> Result<RecordBatch> {
        let full_batch = self.slice_rows_native(file_path, offset, limit)?;
        if selected_cols.is_empty() {
            return Ok(full_batch);
        }

        let schema = full_batch.schema();
        let mut indices = Vec::new();

        for col_name in selected_cols {
            if let Ok(idx) = schema.index_of(col_name) {
                indices.push(idx);
            } else {
                return Err(anyhow!("Column '{}' not found in schema", col_name));
            }
        }

        full_batch.project(&indices).map_err(|e| anyhow!(e))
    }
}
