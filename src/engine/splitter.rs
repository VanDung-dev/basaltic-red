use arrow::array::RecordBatch;
use arrow_csv::WriterBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

use crate::engine::MatrixEngine;
use crate::error::BazanError;

impl MatrixEngine {
    /// Split a large matrix file into smaller part files (part-001, part-002, ...)
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

        let mut part_index = 1;
        let mut current_offset = 0;
        let mut total_written_parts = 0;

        loop {
            let batch = match self.slice_rows_native(file_path, current_offset, max_rows_per_file) {
                Ok(b) if b.num_rows() > 0 => b,
                _ => break,
            };

            let rows_read = batch.num_rows();
            let part_filename = format!("{}_part_{:03}.{}", stem, part_index, format);
            let part_path = Path::new(output_dir).join(part_filename);

            self.write_batch_to_file(&batch, part_path.to_str().unwrap(), format)?;

            part_index += 1;
            current_offset += rows_read;
            total_written_parts += 1;

            if rows_read < max_rows_per_file {
                break;
            }
        }

        Ok(total_written_parts)
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
