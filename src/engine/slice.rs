use arrow::array::RecordBatch;

use crate::engine::formats::handler_for;
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

        let handler = handler_for(&ext).ok_or_else(|| {
            BazanError::Message(format!("Format '.{}' slicing not supported yet", ext))
        })?;

        handler.read_range(file_path, offset, limit, DEFAULT_MAX_BATCH_SIZE)
    }

    /// Read selected columns & row range (Column Projection zero-copy)
    pub fn slice_cols_native(
        &self,
        file_path: &str,
        selected_cols: &[String],
        offset: usize,
        limit: usize,
    ) -> Result<RecordBatch, BazanError> {
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
                return Err(BazanError::Message(format!(
                    "Column '{}' not found in schema",
                    col_name
                )));
            }
        }

        Ok(full_batch.project(&indices)?)
    }
}
