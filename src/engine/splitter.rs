use arrow::array::RecordBatch;
use arrow_csv::WriterBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

use crate::engine::formats::{clamp_batch_size, handler_for, maybe_hint_not_parquet};
use crate::engine::slice::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::MatrixEngine;
use crate::error::BazanError;

impl MatrixEngine {
    /// Split a large matrix file into smaller part files (part-001, part-002, ...).
    /// Single pass over the source: rows are consumed once, carried across batch
    /// boundaries, and emitted as soon as a full part accumulates.
    pub fn split_file_native(
        &self,
        file_path: &str,
        max_rows_per_file: usize,
        output_dir: &str,
        format: &str,
    ) -> Result<usize, BazanError> {
        std::fs::create_dir_all(output_dir)?;
        let path = Path::new(file_path);
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("part");

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase());
        maybe_hint_not_parquet(file_path, ext.as_deref().unwrap_or(""));
        let handler = ext
            .as_deref()
            .and_then(handler_for)
            .ok_or_else(|| BazanError::Message(format!("Unsupported file format: {:?}", ext)))?;
        let source = handler.open(file_path, clamp_batch_size(DEFAULT_MAX_BATCH_SIZE))?;

        let mut part_index = 1;
        let mut total_written_parts = 0;
        let mut carry: Option<RecordBatch> = None;

        for batch_res in source.batches {
            let batch = batch_res?;
            let rows = match carry.take() {
                Some(prev) => arrow::compute::concat_batches(&prev.schema(), &[prev, batch])?,
                None => batch,
            };
            let mut start = 0usize;
            while start + max_rows_per_file <= rows.num_rows() {
                self.write_part(stem, part_index, format, output_dir, &rows.slice(start, max_rows_per_file))?;
                start += max_rows_per_file;
                part_index += 1;
                total_written_parts += 1;
            }
            if start < rows.num_rows() {
                carry = Some(rows.slice(start, rows.num_rows() - start));
            }
        }

        if let Some(last) = carry {
            self.write_part(stem, part_index, format, output_dir, &last)?;
            total_written_parts += 1;
        }

        Ok(total_written_parts)
    }

    fn write_part(
        &self,
        stem: &str,
        part_index: usize,
        format: &str,
        output_dir: &str,
        batch: &RecordBatch,
    ) -> Result<(), BazanError> {
        let part_filename = format!("{}_part_{:03}.{}", stem, part_index, format);
        let part_path = Path::new(output_dir).join(part_filename);
        self.write_batch_to_file(batch, &part_path.to_string_lossy(), format)
    }

    /// Helper to write a RecordBatch to specified format file
    fn write_batch_to_file(
        &self,
        batch: &RecordBatch,
        output_path: &str,
        format: &str,
    ) -> Result<(), BazanError> {
        let file = File::create(output_path)?;
        match format.to_lowercase().as_str() {
            "parquet" | "pq" => {
                let props = WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(
                        parquet::basic::ZstdLevel::default(),
                    ))
                    .build();
                let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
                writer.write(batch)?;
                writer.close()?;
            }
            "csv" => {
                let mut writer = WriterBuilder::new().with_header(true).build(file);
                writer.write(&crate::engine::csv_guard::sanitize_csv_batch(batch))?;
            }

            _ => {
                return Err(BazanError::Message(format!(
                    "Unsupported output format: {}",
                    format
                )))
            }
        }
        Ok(())
    }
}
