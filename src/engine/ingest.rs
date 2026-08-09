use std::fs;
use std::path::{Path, PathBuf};

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

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
        let mut rows_ingested = 0usize;

        for file in &files {
            let rel = file.strip_prefix(src).map_err(|_| {
                BazanError::Message(format!("Ingest source outside {}", src_dir))
            })?;
            let ext = file
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_lowercase();

            let target = if normalize && is_row_format(&ext) {
                self.ingest_normalize(file, dst.join(rel).with_extension("parquet"))?
            } else {
                let target = dst.join(rel);
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(file, &target).map_err(BazanError::from)?;
                0
            };
            rows_ingested += target;
        }

        Ok((files.len(), rows_ingested))
    }

    /// Stream `src` through its format handler and write Parquet to `target`.
    /// Returns the number of rows written.
    fn ingest_normalize(
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
