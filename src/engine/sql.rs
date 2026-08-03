use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;

use datafusion::prelude::*;
use datafusion::datasource::MemTable;

use crate::engine::MatrixEngine;
use crate::engine::container::{read_bazan_manifest, read_bazan_entry_batch};

/// Convert arrow v59 RecordBatch to datafusion's internal RecordBatch via IPC Stream
pub fn to_df_batch(batch: &RecordBatch) -> Result<datafusion::arrow::array::RecordBatch> {
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &batch.schema())?;
    writer.write(batch)?;
    let bytes = writer.into_inner()?;

    let cursor = Cursor::new(bytes);
    let mut reader = datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
    if let Some(res) = reader.next() {
        Ok(res?)
    } else {
        Err(anyhow!("Failed to convert batch to DataFusion Arrow format"))
    }
}

/// Convert datafusion's internal RecordBatch back to arrow v59 RecordBatch via IPC Stream
pub fn from_df_batch(batch: &datafusion::arrow::array::RecordBatch) -> Result<RecordBatch> {
    let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &batch.schema())?;
    writer.write(batch)?;
    let bytes = writer.into_inner()?;

    let cursor = Cursor::new(bytes);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
    if let Some(res) = reader.next() {
        Ok(res?)
    } else {
        Err(anyhow!("Failed to convert batch from DataFusion Arrow format"))
    }
}

impl MatrixEngine {
    /// Execute SQL query directly on .bazan container files, Parquet/CSV files, or directory trees
    pub async fn execute_sql(&self, query_str: &str) -> Result<RecordBatch> {
        let ctx = SessionContext::new();
        let mut modified_query = query_str.to_string();

        // Automatically detect path enclosed in single quotes `'path/file'`
        if let Some(start_idx) = query_str.find('\'') {
            if let Some(end_rel) = query_str[start_idx + 1..].find('\'') {
                let path_str = &query_str[start_idx + 1..start_idx + 1 + end_rel];
                let path_obj = Path::new(path_str);

                if path_obj.exists() {
                    let table_name = "bazan_target";

                    if path_obj.is_file() && path_obj.extension().and_then(|s| s.to_str()) == Some("bazan") {
                        // Register .bazan container entries
                        let manifest = read_bazan_manifest(path_obj)?;
                        let mut df_batches = Vec::new();

                        for entry in manifest.entries {
                            let batch = read_bazan_entry_batch(path_obj, &entry)?;
                            let df_batch = to_df_batch(&batch)?;
                            df_batches.push(df_batch);
                        }

                        if df_batches.is_empty() {
                            return Err(anyhow!("No valid batches found inside .bazan container"));
                        }

                        let schema = df_batches[0].schema();
                        let mem_table = MemTable::try_new(schema, vec![df_batches])?;
                        ctx.register_table(table_name, Arc::new(mem_table))?;
                    } else if path_obj.is_file() && path_obj.extension().and_then(|s| s.to_str()) == Some("parquet") {
                        ctx.register_parquet(table_name, path_str, ParquetReadOptions::default()).await?;
                    } else if path_obj.is_file() && path_obj.extension().and_then(|s| s.to_str()) == Some("csv") {
                        ctx.register_csv(table_name, path_str, CsvReadOptions::default()).await?;
                    } else if path_obj.is_dir() {
                        ctx.register_parquet(table_name, path_str, ParquetReadOptions::default()).await?;
                    }

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
            return Err(anyhow!("SQL query executed successfully but returned 0 rows"));
        }

        let mut arrow_batches = Vec::with_capacity(df_batches.len());
        for df_batch in df_batches {
            let batch = from_df_batch(&df_batch)?;
            arrow_batches.push(batch);
        }

        let schema = arrow_batches[0].schema();
        let concatenated = concat_batches(&schema, &arrow_batches)?;
        Ok(concatenated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sql_query_on_bazan_container() -> Result<()> {
        let dir = tempdir()?;
        let input_dir = dir.path().join("input_db");
        let output_bazan = dir.path().join("test_sql.bazan");

        std::fs::create_dir_all(&input_dir)?;
        std::fs::write(input_dir.join("data.csv"), "id,age,salary\n1,25,1000\n2,15,500\n3,30,1200\n")?;

        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        engine.pack_directory_to_bazan(&input_dir, &output_bazan)?;

        let sql = format!("SELECT id, salary FROM '{}' WHERE age >= 18 ORDER BY salary DESC", output_bazan.display());
        let result = engine.execute_sql(&sql).await?;

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        Ok(())
    }
}
