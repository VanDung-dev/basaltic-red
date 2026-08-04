use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use glob::glob;
use rayon::prelude::*;
use std::path::{Path, PathBuf};

use crate::engine::dynamic_filter::FilterRule;
use crate::engine::partition::discover_and_prune_files;
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
    /// Multi-threaded parallel file filtering engine powered by Rayon, Stream Partition Pruning & .bazan Container
    pub fn filter_files_parallel_native(
        &self,
        path_pattern: &str,
        rules: &[FilterRule],
        explicit_partition_filter: Option<&str>,
        num_threads: Option<usize>,
    ) -> Result<ParallelFilterSummary, BazanError> {
        let path_obj = Path::new(path_pattern);

        // Hỗ trợ lọc trực tiếp trên file Container .bazan
        if path_obj.is_file() && path_obj.extension().and_then(|s| s.to_str()) == Some("bazan") {
            use crate::engine::container::{read_bazan_entry_batch, read_bazan_manifest};
            use crate::engine::partition::{matches_partition_rules, parse_path_partitions};

            let manifest = read_bazan_manifest(path_obj)?;
            let _total_entries = manifest.entries.len();

            let mut target_entries = Vec::new();
            let mut pruned_dirs = 0;

            for entry in manifest.entries {
                let entry_path = Path::new(&entry.path);
                let entry_partitions = parse_path_partitions(entry_path);

                if !matches_partition_rules(&entry_partitions, rules) {
                    pruned_dirs += 1;
                    continue;
                }

                if let Some(filter_str) = explicit_partition_filter {
                    if !entry.path.contains(filter_str) {
                        pruned_dirs += 1;
                        continue;
                    }
                }

                target_entries.push(entry);
            }

            if target_entries.is_empty() {
                return Err(BazanError::Message(format!(
                    "No matching table entries found inside .bazan container: '{}'",
                    path_pattern
                )));
            }

            let total_files = target_entries.len();

            let process_fn = || -> Result<Vec<(RecordBatch, RecordBatch)>, BazanError> {
                target_entries
                    .par_iter()
                    .map(|entry| -> Result<(RecordBatch, RecordBatch), BazanError> {
                        let batch = read_bazan_entry_batch(path_obj, entry)?;
                        let (clean, trash) = self.filter_batch_dynamic(&batch, rules)?;
                        Ok((clean, trash))
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
            let mut clean_batches = Vec::with_capacity(results.len());
            let mut trash_batches = Vec::with_capacity(results.len());

            for (clean, trash) in results {
                clean_rows += clean.num_rows();
                trash_rows += trash.num_rows();

                if clean.num_rows() > 0 {
                    clean_batches.push(clean);
                }
                if trash.num_rows() > 0 {
                    trash_batches.push(trash);
                }
            }

            let total_rows = clean_rows + trash_rows;
            let clean_batch = if !clean_batches.is_empty() {
                Some(concat_batches(&clean_batches[0].schema(), &clean_batches)?)
            } else {
                None
            };
            let trash_batch = if !trash_batches.is_empty() {
                Some(concat_batches(&trash_batches[0].schema(), &trash_batches)?)
            } else {
                None
            };

            return Ok(ParallelFilterSummary {
                total_files,
                pruned_dirs,
                total_rows,
                clean_rows,
                trash_rows,
                clean_batch,
                trash_batch,
            });
        }

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
        let process_fn = || -> Result<Vec<(RecordBatch, RecordBatch)>, BazanError> {
            files
                .par_iter()
                .map(
                    |file_path| -> Result<(RecordBatch, RecordBatch), BazanError> {
                        let file_str = file_path.to_str().ok_or_else(|| {
                            BazanError::Message("Invalid file path string".to_string())
                        })?;
                        let batch = self.slice_rows_native(file_str, 0, usize::MAX)?;
                        let (clean, trash) = self.filter_batch_dynamic(&batch, rules)?;
                        Ok((clean, trash))
                    },
                )
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

        let mut clean_batches = Vec::with_capacity(results.len());
        let mut trash_batches = Vec::with_capacity(results.len());

        for (clean, trash) in results {
            clean_rows += clean.num_rows();
            trash_rows += trash.num_rows();

            if clean.num_rows() > 0 {
                clean_batches.push(clean);
            }
            if trash.num_rows() > 0 {
                trash_batches.push(trash);
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
            writer.write(batch)?;
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
