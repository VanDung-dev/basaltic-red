use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;
use std::fs::{File, create_dir_all, write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use rayon::prelude::*;

use crate::engine::MatrixEngine;
use crate::utils::discover_parquet_files;

fn write_batch_to_file(
    batch: &arrow::array::RecordBatch,
    out_path: &Path,
    writer: &mut Option<ArrowWriter<File>>,
    writer_props: &WriterProperties,
    counter: &mut usize,
) {
    if batch.num_rows() > 0 {
        *counter += batch.num_rows();
        if writer.is_none() {
            if let Some(parent) = out_path.parent() {
                let _ = create_dir_all(parent);
            }
            if let Ok(f) = File::create(out_path) {
                *writer = ArrowWriter::try_new(f, batch.schema(), Some(writer_props.clone())).ok();
            }
        }
        if let Some(ref mut w) = writer {
            let _ = w.write(batch);
        }
    }
}

impl MatrixEngine {
    /// Native Multi-Threaded Partition Streamer & Parquet Writer
    pub fn process_and_write_lake_native(
        &self,
        input_dir: &str,
        clean_output_dir: &str,
        trash_output_dir: &str,
        partition_filter: Option<&str>,
        batch_size: usize,
    ) -> Result<(usize, usize, usize, usize), anyhow::Error> {
        let base_input_path = Path::new(input_dir);
        let files = discover_parquet_files(base_input_path, partition_filter)?;
        let total_files = files.len();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let results: Vec<(usize, usize, usize)> = files
            .par_iter()
            .filter_map(|file_path| {
                let file = File::open(file_path).ok()?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?
                    .with_batch_size(batch_size);
                let mut reader = builder.build().ok()?;

                // Compute relative path to preserve Hive partitioning structure
                let rel_path = file_path.strip_prefix(base_input_path).ok()?;
                let clean_out_path = Path::new(clean_output_dir).join(rel_path);
                let trash_out_path = Path::new(trash_output_dir).join(rel_path);

                let mut clean_writer: Option<ArrowWriter<File>> = None;
                let mut trash_writer: Option<ArrowWriter<File>> = None;

                let mut f_total = 0;
                let mut f_clean = 0;
                let mut f_trash = 0;

                for batch_res in reader.by_ref() {
                    if let Ok(batch) = batch_res {
                        let rows = batch.num_rows();
                        f_total += rows;

                        let (c_b, t_b) = self.filter_batch_native(&batch, rows);

                        // Write Clean RecordBatch if non-empty
                        write_batch_to_file(&c_b, &clean_out_path, &mut clean_writer, &writer_props, &mut f_clean);

                        // Write Trash RecordBatch if non-empty
                        write_batch_to_file(&t_b, &trash_out_path, &mut trash_writer, &writer_props, &mut f_trash);
                    }
                }

                if let Some(w) = clean_writer {
                    let _ = w.close();
                }
                if let Some(w) = trash_writer {
                    let _ = w.close();
                }

                Some((f_total, f_clean, f_trash))
            })
            .collect();

        let (total_rows, total_clean, total_trash) = results
            .into_iter()
            .fold((0, 0, 0), |acc, r| (acc.0 + r.0, acc.1 + r.1, acc.2 + r.2));

        Ok((total_files, total_rows, total_clean, total_trash))
    }

    /// Native Clean Gold Table Generator & Version Manifest Writer
    pub fn generate_gold_table_native(
        &self,
        input_dir: &str,
        gold_output_dir: &str,
        table_version: &str,
        partition_filter: Option<&str>,
        batch_size: usize,
    ) -> Result<(usize, usize, String), anyhow::Error> {
        let base_input_path = Path::new(input_dir);
        let files = discover_parquet_files(base_input_path, partition_filter)?;
        let total_files = files.len();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let results: Vec<usize> = files
            .par_iter()
            .filter_map(|file_path| {
                let file = File::open(file_path).ok()?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?
                    .with_batch_size(batch_size);
                let mut reader = builder.build().ok()?;

                let rel_path = file_path.strip_prefix(base_input_path).ok()?;
                let gold_out_path = Path::new(gold_output_dir).join(rel_path);

                let mut gold_writer: Option<ArrowWriter<File>> = None;
                let mut f_clean = 0;

                for batch_res in reader.by_ref() {
                    if let Ok(batch) = batch_res {
                        let rows = batch.num_rows();
                        let (c_b, _) = self.filter_batch_native(&batch, rows);

                        write_batch_to_file(&c_b, &gold_out_path, &mut gold_writer, &writer_props, &mut f_clean);
                    }
                }

                if let Some(w) = gold_writer {
                    let _ = w.close();
                }

                Some(f_clean)
            })
            .collect();

        let total_gold_rows: usize = results.into_iter().sum();

        // Write _gold_metadata.json manifest file for Data Versioning & Lakehouse Time-Travel
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let manifest_content = format!(
            r#"{{
  "table_name": "clean_gold_table",
  "version": "{}",
  "created_at_epoch": {},
  "total_files": {},
  "total_gold_rows": {},
  "engine": "Basaltic-Red 59.1.0 (Rust SIMD)"
}}"#,
            table_version, timestamp, total_files, total_gold_rows
        );

        let manifest_path = Path::new(gold_output_dir).join("_gold_metadata.json");
        if let Some(parent) = manifest_path.parent() {
            let _ = create_dir_all(parent);
        }
        write(&manifest_path, manifest_content)?;

        Ok((total_files, total_gold_rows, manifest_path.to_str().unwrap_or("").to_string()))
    }
}
