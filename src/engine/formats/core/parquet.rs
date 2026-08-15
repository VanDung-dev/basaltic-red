use arrow::array::RecordBatch;
use arrow_array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::fs::{create_dir_all, write, File};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::formats::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use crate::utils::discover_parquet_files;

/// Shared streaming opener for Parquet.
pub fn open_parquet(file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(clamp_batch_size(batch_size))
        .build()?;
    let schema = reader.schema().clone();
    Ok(OpenedSource {
        schema,
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

/// Parquet opener with column projection (ProjectionMask) so wide tables only
/// read the requested column chunks.
pub fn open_parquet_columns(
    file_path: &str,
    batch_size: usize,
    columns: &[String],
) -> Result<OpenedSource, BazanError> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();
    let file_metadata = builder.metadata().file_metadata();
    let schema_descr = file_metadata.schema_descr();

    let mut projected_indices = Vec::new();
    for col_name in columns {
        for (i, field) in schema_descr.columns().iter().enumerate() {
            if field.name() == col_name {
                projected_indices.push(i);
                break;
            }
        }
    }

    let mask = ProjectionMask::leaves(schema_descr, projected_indices);
    let reader = builder
        .with_batch_size(clamp_batch_size(batch_size))
        .with_projection(mask)
        .build()?;

    let mut projected_fields = Vec::new();
    for col_name in columns {
        if let Ok(field) = arrow_schema.field_with_name(col_name) {
            projected_fields.push(field.clone());
        }
    }
    let projected_schema = Arc::new(arrow_schema.project(&projected_fields_indices(
        &arrow_schema,
        columns,
    ))?);

    Ok(OpenedSource {
        schema: projected_schema,
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

fn projected_fields_indices(
    schema: &arrow::datatypes::Schema,
    columns: &[String],
) -> Vec<usize> {
    columns
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect()
}

/// Parquet Streaming In-Memory Reader (Tier 1 Core Standard)
#[derive(Debug, Clone, Copy, Default)]
pub struct ParquetHandler;

impl FormatHandler for ParquetHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_parquet(file_path, batch_size)
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        open_parquet_columns(file_path, batch_size, columns)
    }
}

fn write_batch_to_file(
    batch: &RecordBatch,
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
        _batch_size: usize,
    ) -> Result<(usize, usize, usize, usize), BazanError> {
        let base_input_path = Path::new(input_dir);
        let files = discover_parquet_files(base_input_path, partition_filter)?;
        let total_files = files.len();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let batch_size = crate::engine::memory::budget_batch_rows(files.len());

        let results: Vec<(usize, usize, usize)> = files
            .par_iter()
            .filter_map(|file_path| {
                let file = File::open(file_path).ok()?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .ok()?
                    .with_batch_size(batch_size);
                let mut reader = builder.build().ok()?;

                let rel_path = file_path.strip_prefix(base_input_path).ok()?;
                let clean_out_path = Path::new(clean_output_dir).join(rel_path);
                let trash_out_path = Path::new(trash_output_dir).join(rel_path);

                let mut clean_writer: Option<ArrowWriter<File>> = None;
                let mut trash_writer: Option<ArrowWriter<File>> = None;

                let mut f_total = 0;
                let mut f_clean = 0;
                let mut f_trash = 0;

                for batch in reader.by_ref().flatten() {
                    let rows = batch.num_rows();
                    f_total += rows;

                    let (c_b, t_b) = self.filter_batch_native(&batch, rows);

                    write_batch_to_file(
                        &c_b,
                        &clean_out_path,
                        &mut clean_writer,
                        &writer_props,
                        &mut f_clean,
                    );

                    write_batch_to_file(
                        &t_b,
                        &trash_out_path,
                        &mut trash_writer,
                        &writer_props,
                        &mut f_trash,
                    );
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
    ) -> Result<(usize, usize, String), BazanError> {
        let batch_size = clamp_batch_size(batch_size);
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
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .ok()?
                    .with_batch_size(batch_size);
                let mut reader = builder.build().ok()?;

                let rel_path = file_path.strip_prefix(base_input_path).ok()?;
                let gold_out_path = Path::new(gold_output_dir).join(rel_path);

                let mut gold_writer: Option<ArrowWriter<File>> = None;
                let mut f_clean = 0;

                for batch in reader.by_ref().flatten() {
                    let rows = batch.num_rows();
                    let (c_b, _) = self.filter_batch_native(&batch, rows);

                    write_batch_to_file(
                        &c_b,
                        &gold_out_path,
                        &mut gold_writer,
                        &writer_props,
                        &mut f_clean,
                    );
                }

                if let Some(w) = gold_writer {
                    let _ = w.close();
                }

                Some(f_clean)
            })
            .collect();

        let total_gold_rows: usize = results.into_iter().sum();

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
  "engine": "Basaltic-Red 0.1.0 (Rust SIMD)"
}}"#,
            table_version, timestamp, total_files, total_gold_rows
        );

        let manifest_path = Path::new(gold_output_dir).join("_gold_metadata.json");
        if let Some(parent) = manifest_path.parent() {
            let _ = create_dir_all(parent);
        }
        write(&manifest_path, manifest_content)?;

        Ok((
            total_files,
            total_gold_rows,
            manifest_path.to_str().unwrap_or("").to_string(),
        ))
    }
}
