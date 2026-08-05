use super::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use arrow_array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

impl MatrixEngine {
    /// Helper method to iterate through RecordBatch reader and sum filter statistics
    pub(crate) fn process_reader<I, E>(
        &self,
        reader: I,
    ) -> Result<(usize, usize, usize), BazanError>
    where
        I: IntoIterator<Item = Result<arrow::array::RecordBatch, E>>,
        BazanError: From<E>,
    {
        let mut total_rows = 0;
        let mut total_clean = 0;
        let mut total_trash = 0;

        for batch_result in reader {
            let batch = batch_result?;
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;

            let (clean_b, trash_b) = self.filter_batch_native(&batch, batch_rows);
            total_clean += clean_b.num_rows();
            total_trash += trash_b.num_rows();
        }

        Ok((total_rows, total_clean, total_trash))
    }
}

/// Shared streaming opener for Parquet.
pub(crate) fn open_parquet(file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
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

/// Shared streaming opener for delimiter-separated text files (csv `,`, psv `|`, txt `;`).
pub(crate) fn open_delimited_csv(
    file_path: &str,
    batch_size: usize,
    delimiter: u8,
) -> Result<OpenedSource, BazanError> {
    let file = File::open(file_path)?;
    let format = arrow_csv::reader::Format::default()
        .with_delimiter(delimiter)
        .with_header(true);

    let (schema, _) = format.infer_schema(file, Some(100))?;

    let batch_size = clamp_batch_size(batch_size);
    let file_for_reader = File::open(file_path)?;
    let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_delimiter(delimiter)
        .with_header(true)
        .with_batch_size(batch_size)
        .build(file_for_reader)?;

    Ok(OpenedSource {
        schema: Arc::new(schema),
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

/// Parquet Streaming In-Memory Reader
pub struct ParquetHandler;

impl FormatHandler for ParquetHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_parquet(file_path, batch_size)
    }
}

/// CSV Streaming In-Memory Reader with Schema Inference
pub struct CsvHandler;

impl FormatHandler for CsvHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_delimited_csv(file_path, batch_size, b',')
    }
}
