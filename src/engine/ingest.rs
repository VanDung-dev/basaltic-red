use std::fs;
use std::path::{Path, PathBuf};

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::engine::formats::handler_for;
use crate::engine::memory::BUDGET_BATCH_ROWS;
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_data_files;

/// Formats that ship rows in a single file (every row read into one batch
/// stream) and benefit from normalization into a columnar store.
fn is_row_format(ext: &str) -> bool {
    matches!(
        ext,
        "csv" | "tsv" | "psv" | "txt" | "json" | "jsonl" | "ndjson" | "msgpack" | "xlsx"
    )
}

impl MatrixEngine {
    /// Ingest a source directory into a destination lake directory, preserving
    /// the relative directory layout. With `auto_normalize` (or the
    /// `BR_INGEST_NORMALIZE=1` env var), row-based formats are converted to
    /// Parquet; other files are copied byte-for-byte.
    ///
    /// Returns `(files_ingested, rows_ingested)`.
    pub fn ingest_native(
        &self,
        src_dir: &str,
        dst_dir: &str,
        auto_normalize: Option<bool>,
    ) -> Result<(usize, usize), BazanError> {
        let normalize = auto_normalize.unwrap_or_else(|| {
            std::env::var("BR_INGEST_NORMALIZE")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
        });

        let src = Path::new(src_dir);
        let dst = Path::new(dst_dir);
        fs::create_dir_all(dst)?;

        let files = discover_data_files(src, None)?;

        // Precompute (source, target) per file, then stream/convert in parallel.
        // Two files sharing a target (a.csv normalized to a.parquet while
        // a.parquet is copied) would overwrite each other. Copy files reserve
        // their natural target first; normalized row files yield when taken.
        let normalize_this: Vec<bool> = files
            .iter()
            .map(|file| {
                let ext = file
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                normalize && is_row_format(&ext)
            })
            .collect();
        let mut seen = std::collections::HashSet::new();
        for (file, norm) in files.iter().zip(&normalize_this) {
            if !*norm {
                let rel = file.strip_prefix(src).map_err(|_| {
                    BazanError::Message(format!("Ingest source outside {}", src_dir))
                })?;
                seen.insert(dst.join(rel));
            }
        }
        let jobs: Vec<(PathBuf, PathBuf, bool)> = files
            .iter()
            .zip(&normalize_this)
            .map(|(file, norm)| {
                let rel = file.strip_prefix(src).map_err(|_| {
                    BazanError::Message(format!("Ingest source outside {}", src_dir))
                })?;
                let ext = file
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                let target = if *norm {
                    let plain = dst.join(rel).with_extension("parquet");
                    if seen.contains(&plain) {
                        dst.join(rel).with_extension(format!("{ext}.parquet"))
                    } else {
                        seen.insert(plain.clone());
                        plain
                    }
                } else {
                    dst.join(rel)
                };
                Ok((file.clone(), target, *norm))
            })
            .collect::<Result<Vec<_>, BazanError>>()?;

        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(jobs.len().max(1));
        let rows = crate::engine::memory::global_rayon_pool(threads).install(|| {
            jobs.par_iter()
                .map(|(file, target, should_normalize)| -> Result<usize, BazanError> {
                    if *should_normalize {
                        self.ingest_normalize(file, target.clone())
                    } else {
                        if let Some(parent) = target.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        fs::copy(file, target).map_err(BazanError::from)?;
                        Ok(0)
                    }
                })
                .collect::<Result<Vec<_>, _>>()
        })?;

        let rows_ingested: usize = rows.iter().sum();
        Ok((files.len(), rows_ingested))
    }

    /// Stream `src` through its format handler and write Parquet to `target`.
    /// Returns the number of rows written.
    pub(crate) fn ingest_normalize(
        &self,
        src: &Path,
        target: PathBuf,
    ) -> Result<usize, BazanError> {
        let file_str = src.to_str().ok_or_else(|| {
            BazanError::Message("Invalid file path string".to_string())
        })?;
        let ext = src
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();
        let handler = handler_for(&ext).ok_or_else(|| {
            BazanError::Message(format!("Unsupported format: .{}", ext))
        })?;

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let out = fs::File::create(&target)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let source = handler.open(file_str, BUDGET_BATCH_ROWS)?;
        let mut writer = None::<ArrowWriter<fs::File>>;
        let mut rows = 0usize;
        for batch_res in source.batches {
            let batch = batch_res?;
            rows += batch.num_rows();
            if writer.is_none() {
                writer = Some(ArrowWriter::try_new(
                    out.try_clone().map_err(BazanError::from)?,
                    batch.schema(),
                    Some(props.clone()),
                )?);
            }
            if let Some(w) = writer.as_mut() {
                w.write(&batch)?;
            }
        }
        if let Some(w) = writer.take() {
            w.close()?;
        }
        Ok(rows)
    }
}
