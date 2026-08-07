use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use glob::glob;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

use crate::engine::dynamic_filter::FilterRule;
use crate::engine::formats::handler_for;
use crate::engine::partition::discover_and_prune_files;
use crate::engine::slice::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::MatrixEngine;
use crate::error::BazanError;

#[derive(Debug)]
pub struct ParallelFilterSummary {
    pub total_files: usize,
    pub pruned_dirs: usize,
    pub total_rows: usize,
    pub clean_rows: usize,
    pub trash_rows: usize,
    pub clean_batch: Option<RecordBatch>,
    pub trash_batch: Option<RecordBatch>,
}

/// (clean batches, trash batches) produced for one file.
type FilteredFile = (Vec<RecordBatch>, Vec<RecordBatch>);

/// Collect files to process based on single file path, directory path, or glob pattern, applying partition pruning
pub fn collect_target_files(
    path_pattern: &str,
    rules: &[FilterRule],
    explicit_partition_filter: Option<&str>,
) -> Result<(Vec<PathBuf>, usize), BazanError> {
    let is_glob =
        path_pattern.contains('*') || path_pattern.contains('?') || path_pattern.contains('[');

    if is_glob {
        let mut files = Vec::new();
        for entry in glob(path_pattern)? {
            let path = entry?;
            if path.is_file() {
                files.push(path);
            }
        }
        Ok((files, 0))
    } else {
        let path = Path::new(path_pattern);
        if !path.exists() {
            return Err(BazanError::Message(format!(
                "Path does not exist: {}",
                path_pattern
            )));
        }

        if path.is_file() {
            Ok((vec![path.to_path_buf()], 0))
        } else if path.is_dir() {
            let (discovered, pruned_dirs) =
                discover_and_prune_files(path, rules, explicit_partition_filter)?;
            Ok((discovered, pruned_dirs))
        } else {
            Err(BazanError::Message(format!(
                "Invalid target path: {}",
                path_pattern
            )))
        }
    }
}

impl MatrixEngine {
    /// Multi-threaded parallel file filtering engine powered by Rayon & Stream Partition Pruning
    pub fn filter_files_parallel_native(
        &self,
        path_pattern: &str,
        rules: &[FilterRule],
        explicit_partition_filter: Option<&str>,
        num_threads: Option<usize>,
    ) -> Result<ParallelFilterSummary, BazanError> {
        let (files, pruned_dirs) =
            collect_target_files(path_pattern, rules, explicit_partition_filter)?;
        if files.is_empty() {
            return Err(BazanError::Message(format!(
                "No valid data files found matching path: '{}'",
                path_pattern
            )));
        }

        let total_files = files.len();

        // Optional thread pool configuration
        let process_fn = || -> Result<Vec<FilteredFile>, BazanError> {
            files
                .par_iter()
                .map(|file_path| -> Result<FilteredFile, BazanError> {
                    let file_str = file_path.to_str().ok_or_else(|| {
                        BazanError::Message("Invalid file path string".to_string())
                    })?;
                    let path = Path::new(file_str);
                    let ext = path
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_lowercase();
                    let handler = handler_for(&ext).ok_or_else(|| {
                        BazanError::Message(format!("Unsupported format: .{}", ext))
                    })?;

                    // Stream per-batch instead of loading the whole file into RAM
                    let source = handler.open(file_str, DEFAULT_MAX_BATCH_SIZE)?;
                    let mut clean_batches = Vec::new();
                    let mut trash_batches = Vec::new();
                    for batch_res in source.batches {
                        let batch = batch_res?;
                        let (clean, trash) = self.filter_batch_dynamic(&batch, rules)?;
                        if clean.num_rows() > 0 {
                            clean_batches.push(clean);
                        }
                        if trash.num_rows() > 0 {
                            trash_batches.push(trash);
                        }
                    }
                    Ok((clean_batches, trash_batches))
                })
                .collect()
        };

        let results = if let Some(threads) = num_threads {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()?;
            pool.install(process_fn)?
        } else {
            process_fn()?
        };

        let mut clean_rows = 0;
        let mut trash_rows = 0;

        let mut clean_batches = Vec::new();
        let mut trash_batches = Vec::new();

        for (file_clean, file_trash) in results {
            for batch in file_clean {
                clean_rows += batch.num_rows();
                clean_batches.push(batch);
            }
            for batch in file_trash {
                trash_rows += batch.num_rows();
                trash_batches.push(batch);
            }
        }
        let total_rows = clean_rows + trash_rows;

        // Concatenate clean batches
        let clean_batch = if !clean_batches.is_empty() {
            let schema = clean_batches[0].schema();
            Some(concat_batches(&schema, &clean_batches)?)
        } else {
            None
        };

        // Concatenate trash batches
        let trash_batch = if !trash_batches.is_empty() {
            let schema = trash_batches[0].schema();
            Some(concat_batches(&schema, &trash_batches)?)
        } else {
            None
        };

        Ok(ParallelFilterSummary {
            total_files,
            pruned_dirs,
            total_rows,
            clean_rows,
            trash_rows,
            clean_batch,
            trash_batch,
        })
    }
}

/// Helper function to persist output RecordBatch to specified destination file path
pub fn save_batch_to_file(batch: &RecordBatch, out_path: &Path) -> Result<(), BazanError> {
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let ext = out_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "csv" => {
            let file = std::fs::File::create(out_path)?;
            let mut writer = arrow::csv::Writer::new(file);
            writer.write(&crate::engine::csv_guard::sanitize_csv_batch(batch))?;
        }
        "json" | "jsonl" | "ndjson" => {
            let file = std::fs::File::create(out_path)?;
            let mut writer = arrow::json::LineDelimitedWriter::new(file);
            writer.write(batch)?;
        }
        _ => {
            // Default to Parquet format
            let file = std::fs::File::create(out_path)?;
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
        }
    }

    Ok(())
}
