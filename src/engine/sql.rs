use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::concat_batches;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::{
    arrow::ArrowFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat,
};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;

use crate::engine::formats::{handler_for, maybe_hint_not_parquet, resolve_handler_for_file};
use crate::engine::slice::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_data_files;

/// Map a file extension to a DataFusion native `FileFormat`, or `None` when
/// the format has no native DataFusion reader (msgpack/xlsx/orc/txt/mixed).
fn listing_format_for(ext: &str) -> Option<Arc<dyn FileFormat>> {
    let format: Arc<dyn FileFormat> = match ext {
        "parquet" | "pq" => Arc::new(ParquetFormat::new()),
        "csv" => Arc::new(CsvFormat::default().with_delimiter(b',')),
        "tsv" => Arc::new(CsvFormat::default().with_delimiter(b'\t')),
        "psv" => Arc::new(CsvFormat::default().with_delimiter(b'|')),
        "json" | "jsonl" | "ndjson" => Arc::new(JsonFormat::default()),
        "arrow" | "ipc" | "feather" => Arc::new(ArrowFormat),
        _ => return None,
    };
    Some(format)
}

/// First non-whitespace byte of `path`, used to detect top-level JSON arrays
/// (which `JsonFormat` cannot read — it expects newline-delimited objects).
fn first_non_ws_byte(path: &Path) -> Option<u8> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = [0u8; 1024];
    let n = f.read(&mut buf).ok()?;
    buf[..n].iter().copied().find(|b| !b.is_ascii_whitespace())
}

/// DataFusion native reader for `ext`; `None` for msgpack/xlsx/orc/txt/mixed,
/// and for JSON files that are top-level arrays (handled by the streaming
/// handler instead).
fn native_reader_for(path: &Path, ext: &str) -> Option<Arc<dyn FileFormat>> {
    if matches!(ext, "json" | "jsonl" | "ndjson") && first_non_ws_byte(path) == Some(b'[') {
        return None;
    }
    listing_format_for(ext)
}

/// Register `path` (a file or a homogeneous-extension directory) as a
/// DataFusion listing table named `br_target`. Returns `true` when a native
/// listing table was registered, `false` when the caller should fall back to
/// the in-memory handler path.
async fn register_listing_table(
    ctx: &SessionContext,
    path: &Path,
    ext: &str,
) -> Result<bool, BazanError> {
    let Some(format) = native_reader_for(path, ext) else {
        return Ok(false);
    };
    let abs_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let url = ListingTableUrl::parse(abs_path.to_str().unwrap_or(""))
        .map_err(|e| BazanError::Message(format!("listing url: {e}")))?;
    let options = ListingOptions::new(format).with_file_extension(format!(".{ext}"));
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .map_err(|e| BazanError::Message(format!("listing schema: {e}")))?;
    let table = ListingTable::try_new(config)
        .map_err(|e| BazanError::Message(format!("listing table: {e}")))?;
    ctx.register_table("br_target", Arc::new(table))
        .map_err(|e| BazanError::Message(format!("register table: {e}")))?;
    Ok(true)
}

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

/// `BASALTIC_RED_AUTO_NORMALIZE=1` gates the SQL-side transcoding cache.
fn auto_normalize_enabled() -> bool {
    std::env::var("BASALTIC_RED_AUTO_NORMALIZE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Cache root for `path`: `BASALTIC_RED_CACHE_DIR` override, else `<dir>/.br_cache`
/// where `<dir>` is the path itself (for a directory) or its parent (for a file).
fn auto_cache_root(path: &Path) -> PathBuf {
    if let Ok(dir) = std::env::var("BASALTIC_RED_CACHE_DIR") {
        if !dir.trim().is_empty() {
            return PathBuf::from(dir);
        }
    }
    let dir = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().unwrap_or(Path::new(".")).to_path_buf()
    };
    dir.join(".br_cache")
}

/// Transcode a non-native file into a cached Parquet, reusing an existing
/// cache entry if fresh. Returns the cached file path (registered as a native table).
fn cached_parquet_for(engine: &MatrixEngine, src: &Path) -> Result<PathBuf, BazanError> {
    let root = auto_cache_root(src);
    let stem = src
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data")
        .to_string();
    let ext = src
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    let target = root.join(format!("{stem}.{ext}.parquet"));

    let is_stale = if !target.exists() {
        true
    } else {
        match (
            src.metadata().and_then(|m| m.modified()),
            target.metadata().and_then(|m| m.modified()),
        ) {
            (Ok(src_time), Ok(target_time)) => src_time > target_time,
            _ => false,
        }
    };

    if is_stale {
        engine.ingest_normalize(src, target.clone())?;
    }
    Ok(target)
}

impl MatrixEngine {
    /// Register the `'path'` from `query_str` as a DataFusion table and return
    /// the query with the path replaced by the registered table name. Shared by
    /// the eager (`execute_sql_batches_inner`) and lazy (`execute_sql_stream_inner`)
    /// execution paths.
    async fn prepare_query_context(
        &self,
        query_str: &str,
    ) -> Result<(SessionContext, String), BazanError> {
        let ctx = SessionContext::new();
        let mut modified_query = query_str.to_string();

        // Automatically detect path enclosed in single quotes `'path/file'`
        if let Some(start_idx) = query_str.find('\'') {
            if let Some(end_rel) = query_str[start_idx + 1..].find('\'') {
                let path_str = &query_str[start_idx + 1..start_idx + 1 + end_rel];
                let path_obj = Path::new(path_str);

                if path_obj.exists() {
                    let table_name = "br_target";
                    let mut df_batches: Vec<RecordBatch> = Vec::new();
                    let primary_registered_name = table_name.to_string();

                    let mut register_source =
                        |handler: std::sync::Arc<dyn crate::engine::formats::FormatHandler>,
                         file_str: &str|
                         -> Result<(), BazanError> {
                            let ext = std::path::Path::new(file_str)
                                .extension()
                                .and_then(|s| s.to_str())
                                .unwrap_or("")
                                .to_lowercase();
                            crate::engine::formats::maybe_hint_not_parquet(file_str, &ext);
                            let source = handler.open(file_str, DEFAULT_MAX_BATCH_SIZE)?;
                            for batch_res in source.batches {
                                df_batches.push(batch_res?);
                            }
                            Ok(())
                        };

                    // `None` when the file was registered natively (no handler fallback needed).
                    if path_obj.is_file() {
                        let ext = path_obj
                            .extension()
                            .and_then(|s| s.to_str())
                            .unwrap_or("")
                            .to_lowercase();
                        maybe_hint_not_parquet(path_str, &ext);
                        // Fast path: DataFusion native reader (parquet/csv/json/...)
                        // with pushdown + row-group parallelism. Falls back to the
                        // in-memory handler when the format has no native reader.
                        if !register_listing_table(&ctx, path_obj, &ext).await? {
                            // Auto-normalize cache: transcode non-native files to
                            // Parquet once, then query the cached copy natively.
                            let auto_cached = if auto_normalize_enabled() {
                                cached_parquet_for(self, path_obj).ok()
                            } else {
                                None
                            };
                            let registered = match &auto_cached {
                                Some(cache) => register_listing_table(&ctx, cache, "parquet").await?,
                                None => false,
                            };
                            if !registered {
                                let handler = resolve_handler_for_file(path_str).ok_or_else(|| {
                                    BazanError::Message(format!("Unsupported format: .{}", ext))
                                })?;
                                register_source(handler, path_str)?;
                            }
                        }
                    } else if path_obj.is_dir() {
                        let files = discover_data_files(path_obj, None)?;
                        let ext = files
                            .first()
                            .map(|f| {
                                f.extension()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or("")
                                    .to_lowercase()
                            })
                            .unwrap_or_default();
                        let homogeneous = !files.is_empty()
                            && files.iter().all(|f| {
                                let e = f
                                    .extension()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or("")
                                    .to_lowercase();
                                e == ext
                            });
                        if homogeneous {
                            maybe_hint_not_parquet(path_str, &ext);
                            if !register_listing_table(&ctx, path_obj, &ext).await? {
                                // ponytail: dir auto-normalize not cached (per-file
                                // transcode would need a cache-dir listing); fall back
                                // to the streaming handler path. Add when dir caches matter.
                                for file in files {
                                    let file_str = file.to_str().ok_or_else(|| {
                                        BazanError::Message(
                                            "Invalid file path string".to_string(),
                                        )
                                    })?;
                                    let file_ext = file
                                        .extension()
                                        .and_then(|s| s.to_str())
                                        .unwrap_or("")
                                        .to_lowercase();
                                    let handler = handler_for(&file_ext).ok_or_else(|| {
                                        BazanError::Message(format!(
                                            "Unsupported format: .{}",
                                            file_ext
                                        ))
                                    })?;
                                    register_source(handler, file_str)?;
                                }
                            }
                        } else {
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
                                    BazanError::Message(format!(
                                        "Unsupported format: .{}",
                                        ext
                                    ))
                                })?;
                                register_source(handler, file_str)?;
                            }
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

        Ok((ctx, modified_query))
    }

    /// Execute SQL query directly on any supported file or directory tree
    pub async fn execute_sql_batches_inner(&self, query_str: &str) -> Result<Vec<RecordBatch>, BazanError> {
        let (ctx, modified_query) = self.prepare_query_context(query_str).await?;

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

    /// Execute SQL lazily: returns the DataFusion `SendableRecordBatchStream`
    /// from `execute_stream()` without collecting up front. Native files stream
    /// lazily; non-native files still load into a MemTable during registration.
    pub async fn execute_sql_stream_inner(
        &self,
        query_str: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, BazanError> {
        let (ctx, modified_query) = self.prepare_query_context(query_str).await?;
        let df = ctx.sql(&modified_query).await?;
        Ok(df.execute_stream().await?)
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
