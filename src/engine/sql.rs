use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::concat_batches;
use std::path::Path;
use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::prelude::*;

use crate::engine::formats::handler_for;
use crate::engine::slice::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_data_files;

/// Reorder batches so every batch matches one canonical schema (union of field
/// names in first-seen order); missing columns become nulls.
fn align_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, BazanError> {
    let mut fields: Vec<Arc<arrow::datatypes::Field>> = Vec::new();
    for batch in &batches {
        for field in batch.schema().fields() {
            if !fields.iter().any(|f| f.name() == field.name()) {
                fields.push(field.clone());
            }
        }
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));

    batches
        .into_iter()
        .map(|batch| {
            let columns: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|f| match batch.column_by_name(f.name()) {
                    Some(col) => Arc::clone(col),
                    None => arrow::array::new_null_array(f.data_type(), batch.num_rows()),
                })
                .collect();
            RecordBatch::try_new(schema.clone(), columns).map_err(BazanError::from)
        })
        .collect()
}

impl MatrixEngine {
    /// Execute SQL query directly on any supported file, .bazan container, or directory tree
    pub async fn execute_sql(&self, query_str: &str) -> Result<RecordBatch, BazanError> {
        let ctx = SessionContext::new();
        let mut modified_query = query_str.to_string();

        // Automatically detect path enclosed in single quotes `'path/file'`
        if let Some(start_idx) = query_str.find('\'') {
            if let Some(end_rel) = query_str[start_idx + 1..].find('\'') {
                let path_str = &query_str[start_idx + 1..start_idx + 1 + end_rel];
                let path_obj = Path::new(path_str);

                if path_obj.exists() {
                    let table_name = "bazan_target";
                    let mut df_batches: Vec<RecordBatch> = Vec::new();

                    let mut register_source =
                        |handler: &'static dyn crate::engine::formats::FormatHandler,
                         file_str: &str|
                         -> Result<(), BazanError> {
                            let source = handler.open(file_str, DEFAULT_MAX_BATCH_SIZE)?;
                            for batch_res in source.batches {
                                df_batches.push(batch_res?);
                            }
                            Ok(())
                        };

                    if path_obj.is_file() {
                        let ext = path_obj
                            .extension()
                            .and_then(|s| s.to_str())
                            .unwrap_or("")
                            .to_lowercase();
                        let handler = handler_for(&ext).ok_or_else(|| {
                            BazanError::Message(format!("Unsupported format: .{}", ext))
                        })?;
                        register_source(handler, path_str)?;
                    } else if path_obj.is_dir() {
                        let files = discover_data_files(path_obj, None)?;
                        for file in files {
                            let file_str = file.to_str().ok_or_else(|| {
                                BazanError::Message("Invalid file path string".to_string())
                            })?;
                            let ext = file
                                .extension()
                                .and_then(|s| s.to_str())
                                .unwrap_or("")
                                .to_lowercase();
                            let handler = handler_for(&ext).ok_or_else(|| {
                                BazanError::Message(format!("Unsupported format: .{}", ext))
                            })?;
                            register_source(handler, file_str)?;
                        }
                    }

                    if df_batches.is_empty() {
                        return Err(BazanError::Message(
                            "No valid batches found for target path".to_string(),
                        ));
                    }

                    // arrow_json sorts fields alphabetically while csv keeps
                    // header order, so align every batch to a canonical schema
                    // (first-seen field order) before handing them to MemTable.
                    let df_batches = align_batches(df_batches)?;

                    let schema = df_batches[0].schema();
                    let mem_table = MemTable::try_new(schema, vec![df_batches])?;
                    ctx.register_table(table_name, Arc::new(mem_table))?;

                    // Replace original `'path'` with registered virtual table name
                    let target_token = format!("'{}'", path_str);
                    modified_query = modified_query.replace(&target_token, table_name);
                }
            }
        }

        // Execute query plan with DataFusion SQL Engine
        let df = ctx.sql(&modified_query).await?;
        let df_batches = df.collect().await?;

        if df_batches.is_empty() {
            return Err(BazanError::Message(
                "SQL query executed successfully but returned 0 rows".to_string(),
            ));
        }

        let schema = df_batches[0].schema();
        let concatenated = concat_batches(&schema, &df_batches)?;
        Ok(concatenated)
    }
}
