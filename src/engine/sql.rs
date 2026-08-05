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

pub fn extract_table_name(entry_path: &str) -> String {
    let path = Path::new(entry_path);
    if let Some(parent) = path.parent() {
        if let Some(parent_name) = parent.file_name().and_then(|s| s.to_str()) {
            if !parent_name.is_empty() && parent_name != "." {
                return parent_name.to_lowercase();
            }
        }
    }
    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
        return stem.to_lowercase();
    }
    entry_path.to_lowercase()
}

impl MatrixEngine {
    /// Execute SQL query directly on any supported file, .bazan container, or directory tree
    pub async fn execute_sql_batches_inner(&self, query_str: &str) -> Result<Vec<RecordBatch>, BazanError> {
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
                    let mut primary_registered_name = table_name.to_string();

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
                        if ext == "bazan" {
                            let manifest = crate::engine::container::read_bazan_manifest(path_obj)?;
                            let mut grouped: std::collections::BTreeMap<String, Vec<crate::engine::container::BazanEntry>> =
                                std::collections::BTreeMap::new();

                            for entry in manifest.entries {
                                let t_name = extract_table_name(&entry.path);
                                grouped.entry(t_name).or_default().push(entry);
                            }

                            let mut first_name = None;
                            for (t_name, t_entries) in &grouped {
                                if first_name.is_none() {
                                    first_name = Some(t_name.clone());
                                }
                                let provider = crate::engine::container::BazanTableProvider::try_new_table(path_obj, t_entries.clone())?;
                                ctx.register_table(t_name, Arc::new(provider))?;
                            }

                            let query_after_path = query_str[start_idx + 1 + end_rel + 1..].trim_start();
                            let words: Vec<&str> = query_after_path.split_whitespace().collect();
                            let mut matched_name = None;

                            if !words.is_empty() {
                                let first_w = words[0].trim_matches(|c: char| !c.is_alphanumeric() && c != '_').to_lowercase();
                                if first_w == "as" && words.len() > 1 {
                                    let second_w = words[1].trim_matches(|c: char| !c.is_alphanumeric() && c != '_').to_lowercase();
                                    if grouped.contains_key(&second_w) {
                                        matched_name = Some(second_w);
                                    }
                                } else if grouped.contains_key(&first_w) {
                                    matched_name = Some(first_w.clone());
                                }
                            }

                            if matched_name.is_none() {
                                let query_lower = query_str.to_lowercase();
                                for t_name in grouped.keys() {
                                    let field_prefix = format!("{}.", t_name);
                                    let join_prefix = format!("join {}", t_name);
                                    if query_lower.contains(&field_prefix) && !query_lower.contains(&join_prefix) {
                                        matched_name = Some(t_name.clone());
                                        break;
                                    }
                                }
                            }

                            if let Some(ref name) = matched_name.or(first_name) {
                                primary_registered_name = name.clone();
                                if let Some(first_entries) = grouped.get(name) {
                                    let primary_provider = crate::engine::container::BazanTableProvider::try_new_table(path_obj, first_entries.clone())?;
                                    ctx.register_table(table_name, Arc::new(primary_provider))?;
                                }
                            }
                        } else {
                            let handler = handler_for(&ext).ok_or_else(|| {
                                BazanError::Message(format!("Unsupported format: .{}", ext))
                            })?;
                            register_source(handler, path_str)?;
                        }
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

                    if !ctx.table_exist(table_name)? && df_batches.is_empty() {
                        return Err(BazanError::Message(
                            "No valid batches found for target path".to_string(),
                        ));
                    }

                    if !df_batches.is_empty() {
                        // arrow_json sorts fields alphabetically while csv keeps
                        // header order, so align every batch to a canonical schema
                        // (first-seen field order) before handing them to MemTable.
                        let df_batches = align_batches(df_batches)?;

                        let schema = df_batches[0].schema();
                        let mem_table = MemTable::try_new(schema, vec![df_batches])?;
                        ctx.register_table(table_name, Arc::new(mem_table))?;
                    }
                    // Replace original `'path'` with registered virtual table name
                    let token_with_alias = format!("'{}' {}", path_str, primary_registered_name);
                    if modified_query.contains(&token_with_alias) {
                        modified_query = modified_query.replace(&token_with_alias, &primary_registered_name);
                    } else {
                        let target_token = format!("'{}'", path_str);
                        modified_query = modified_query.replace(&target_token, &primary_registered_name);
                    }
                }
            }
        }

        // Execute query plan with DataFusion SQL Engine
        let df = ctx.sql(&modified_query).await?;
        let result_batches = df.collect().await?;

        if result_batches.is_empty() {
            return Err(BazanError::Message(
                "SQL query executed successfully but returned 0 rows".to_string(),
            ));
        }

        Ok(result_batches)
    }

    /// Execute SQL query directly and return raw list of RecordBatch streams
    pub async fn execute_sql_batches(&self, query_str: &str) -> Result<Vec<RecordBatch>, BazanError> {
        self.execute_sql_batches_inner(query_str).await
    }

    /// Execute SQL query directly and return concatenated RecordBatch
    pub async fn execute_sql(&self, query_str: &str) -> Result<RecordBatch, BazanError> {
        let batches = self.execute_sql_batches_inner(query_str).await?;
        let schema = batches[0].schema();
        let concatenated = concat_batches(&schema, &batches)?;
        Ok(concatenated)
    }
}
