use arrow::array::RecordBatch;

pub use crate::engine::formats::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::formats::{maybe_hint_not_parquet, resolve_handler_for_file};
use crate::engine::formats::{
    read_arrow_ipc_range, read_delimited_range, read_ndjson_range,
    read_parquet_range_from_row_groups,
};
use crate::engine::map::{
    resolve_arrow_ipc_range, resolve_delimited_range, resolve_ndjson_range, resolve_parquet_range,
};
use crate::engine::MatrixEngine;
use crate::error::BazanError;

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

        if matches!(ext.as_str(), "parquet" | "pq") {
            if let Some(resolved) = resolve_parquet_range(path, offset, limit)? {
                return read_parquet_range_from_row_groups(
                    file_path,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                    &[],
                    &resolved.row_groups,
                );
            }
        }

        if matches!(ext.as_str(), "ipc" | "arrow" | "feather") {
            if let Some(resolved) = resolve_arrow_ipc_range(path, offset, limit)? {
                return read_arrow_ipc_range(
                    file_path,
                    resolved.batch_ordinal,
                    resolved.offset,
                    limit,
                );
            }
        }

        if matches!(ext.as_str(), "csv" | "psv") {
            let delimiter = if ext == "csv" { b',' } else { b'|' };
            if let Some(resolved) = resolve_delimited_range(path, offset, limit, delimiter)? {
                return read_delimited_range(
                    file_path,
                    resolved.byte_offset,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                    delimiter,
                    false,
                );
            }
        }

        if ext == "tsv" {
            if let Some(resolved) = resolve_delimited_range(path, offset, limit, b'\t')? {
                return read_delimited_range(
                    file_path,
                    resolved.byte_offset,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                    b'\t',
                    true,
                );
            }
        }

        if ext == "ndjson" {
            if let Some(resolved) = resolve_ndjson_range(path, offset, limit)? {
                return read_ndjson_range(
                    file_path,
                    resolved.byte_offset,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                );
            }
        }

        let handler = resolve_handler_for_file(file_path).ok_or_else(|| {
            BazanError::Message(format!(
                "Format for '{}' not supported or recognized",
                file_path
            ))
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

        if matches!(ext.as_str(), "parquet" | "pq") {
            if let Some(resolved) = resolve_parquet_range(path, offset, limit)? {
                let batch = read_parquet_range_from_row_groups(
                    file_path,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                    selected_cols,
                    &resolved.row_groups,
                )?;
                let schema = batch.schema();
                let mut indices = Vec::new();
                for col_name in selected_cols {
                    indices.push(schema.index_of(col_name).map_err(|_| {
                        BazanError::Message(format!("Column '{}' not found in schema", col_name))
                    })?);
                }
                return Ok(batch.project(&indices)?);
            }
        }

        if matches!(ext.as_str(), "ipc" | "arrow" | "feather") {
            if let Some(resolved) = resolve_arrow_ipc_range(path, offset, limit)? {
                let batch = read_arrow_ipc_range(
                    file_path,
                    resolved.batch_ordinal,
                    resolved.offset,
                    limit,
                )?;
                let schema = batch.schema();
                let mut indices = Vec::new();
                for col_name in selected_cols {
                    indices.push(schema.index_of(col_name).map_err(|_| {
                        BazanError::Message(format!("Column '{}' not found in schema", col_name))
                    })?);
                }
                return Ok(batch.project(&indices)?);
            }
        }

        if matches!(ext.as_str(), "csv" | "psv") {
            let delimiter = if ext == "csv" { b',' } else { b'|' };
            if let Some(resolved) = resolve_delimited_range(path, offset, limit, delimiter)? {
                let batch = read_delimited_range(
                    file_path,
                    resolved.byte_offset,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                    delimiter,
                    false,
                )?;
                let schema = batch.schema();
                let mut indices = Vec::new();
                for col_name in selected_cols {
                    indices.push(schema.index_of(col_name).map_err(|_| {
                        BazanError::Message(format!("Column '{}' not found in schema", col_name))
                    })?);
                }
                return Ok(batch.project(&indices)?);
            }
        }

        if ext == "tsv" {
            if let Some(resolved) = resolve_delimited_range(path, offset, limit, b'\t')? {
                let batch = read_delimited_range(
                    file_path,
                    resolved.byte_offset,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                    b'\t',
                    true,
                )?;
                let schema = batch.schema();
                let mut indices = Vec::new();
                for col_name in selected_cols {
                    indices.push(schema.index_of(col_name).map_err(|_| {
                        BazanError::Message(format!("Column '{}' not found in schema", col_name))
                    })?);
                }
                return Ok(batch.project(&indices)?);
            }
        }

        if ext == "ndjson" {
            if let Some(resolved) = resolve_ndjson_range(path, offset, limit)? {
                let batch = read_ndjson_range(
                    file_path,
                    resolved.byte_offset,
                    resolved.offset,
                    limit,
                    DEFAULT_MAX_BATCH_SIZE,
                )?;
                let schema = batch.schema();
                let mut indices = Vec::new();
                for col_name in selected_cols {
                    indices.push(schema.index_of(col_name).map_err(|_| {
                        BazanError::Message(format!("Column '{}' not found in schema", col_name))
                    })?);
                }
                return Ok(batch.project(&indices)?);
            }
        }

        let handler = resolve_handler_for_file(file_path).ok_or_else(|| {
            BazanError::Message(format!(
                "Format for '{}' not supported or recognized",
                file_path
            ))
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
