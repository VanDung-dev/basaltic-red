use arrow::array::RecordBatch;

use crate::engine::formats::{handler_for, maybe_hint_not_parquet};
use crate::engine::MatrixEngine;
use crate::error::BazanError;

/// Unified 2 GB RAM Budget Max Batch Size: 2^20 = 1,048,576 rows per batch (~500MB-1.5GB RAM)
pub const DEFAULT_MAX_BATCH_SIZE: usize = 1 << 20;

impl MatrixEngine {
    /// Read a specific row range (offset..offset+limit) zero-copy from any supported format
    pub fn slice_rows_native(
        &self,
        file_path: &str,
        offset: usize,
        limit: usize,
    ) -> Result<RecordBatch, BazanError> {
        let path = std::path::Path::new(file_path);
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        maybe_hint_not_parquet(file_path, &ext);
        let handler = handler_for(&ext).ok_or_else(|| {
            BazanError::Message(format!("Format '.{}' slicing not supported yet", ext))
        })?;

        handler.read_range(file_path, offset, limit, DEFAULT_MAX_BATCH_SIZE)
    }

    /// Read selected columns & row range. Columns are pushed down to the reader
    /// where it supports projection (parquet, csv-family); other formats read
    /// everything and project afterwards. Result columns follow `selected_cols` order.
    pub fn slice_cols_native(
        &self,
        file_path: &str,
        selected_cols: &[String],
        offset: usize,
        limit: usize,
    ) -> Result<RecordBatch, BazanError> {
        if selected_cols.is_empty() {
            return self.slice_rows_native(file_path, offset, limit);
        }

        let path = std::path::Path::new(file_path);
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        maybe_hint_not_parquet(file_path, &ext);
        let handler = handler_for(&ext).ok_or_else(|| {
            BazanError::Message(format!("Format '.{}' slicing not supported yet", ext))
        })?;

        let batch = handler.read_range_columns(
            file_path,
            offset,
            limit,
            DEFAULT_MAX_BATCH_SIZE,
            selected_cols,
        )?;

        // Reader projection preserves original schema order; reorder to requested order.
        let schema = batch.schema();
        let mut indices = Vec::new();
        for col_name in selected_cols {
            indices.push(schema.index_of(col_name).map_err(|_| {
                BazanError::Message(format!("Column '{}' not found in schema", col_name))
            })?);
        }

        Ok(batch.project(&indices)?)
    }
}
