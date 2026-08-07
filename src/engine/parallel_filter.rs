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
    /// Multi-threaded parallel file filtering engine powered by Rayon & Stream Partition Pruning.
    /// Streams each file per-batch and only counts clean/trash rows — no whole-file
    /// concatenation into RAM (Python reads counts only).
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

        // (clean_rows, trash_rows) per file, streamed and counted in parallel.
        let process_fn = || -> Result<(usize, usize), BazanError> {
            let counts = files
                .par_iter()
                .map(|file_path| -> Result<(usize, usize), BazanError> {
                    let file_str = file_path.to_str().ok_or_else(|| {
                        BazanError::Message("Invalid file path string".to_string())
                    })?;
                    let ext = Path::new(file_str)
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_lowercase();
                    let handler = handler_for(&ext).ok_or_else(|| {
                        BazanError::Message(format!("Unsupported format: .{}", ext))
                    })?;

                    let source = handler.open(file_str, DEFAULT_MAX_BATCH_SIZE)?;
                    let mut clean_rows = 0usize;
                    let mut trash_rows = 0usize;
                    for batch_res in source.batches {
                        let batch = batch_res?;
                        let (clean, trash) = self.filter_batch_dynamic(&batch, rules)?;
                        clean_rows += clean.num_rows();
                        trash_rows += trash.num_rows();
                    }
                    Ok((clean_rows, trash_rows))
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(counts
                .iter()
                .fold((0usize, 0usize), |(c, t), (cc, tt)| (c + cc, t + tt)))
        };

        let (clean_rows, trash_rows) = if let Some(threads) = num_threads {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()?;
            pool.install(process_fn)?
        } else {
            process_fn()?
        };
        let total_rows = clean_rows + trash_rows;

        Ok(ParallelFilterSummary {
            total_files,
            pruned_dirs,
            total_rows,
            clean_rows,
            trash_rows,
        })
    }
}
