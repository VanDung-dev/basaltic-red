use super::{clamp_batch_size, FormatHandler};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
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

    /// Shared streaming reader for delimiter-separated text files (csv `,`, psv `|`, txt `;`).
    pub(crate) fn process_delimited_csv(
        &self,
        file_path: &str,
        batch_size: usize,
        delimiter: u8,
    ) -> Result<(usize, usize, usize), BazanError> {
        let file = File::open(file_path)?;
        let format = arrow_csv::reader::Format::default()
            .with_delimiter(delimiter)
            .with_header(true);

        let (schema, _) = format.infer_schema(file, Some(100))?;

        let batch_size = clamp_batch_size(batch_size);
        let file_for_reader = File::open(file_path)?;
        let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_delimiter(delimiter)
            .with_header(true)
            .with_batch_size(batch_size)
            .build(file_for_reader)?;

        self.process_reader(reader)
    }

    /// Shared streaming reader for Parquet and ORC columnar files.
    pub(crate) fn process_parquet_stream(
        &self,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let file = File::open(file_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(clamp_batch_size(batch_size))
            .build()?;
        self.process_reader(reader)
    }
}

/// Parquet Streaming In-Memory Reader
pub struct ParquetHandler;

impl FormatHandler for ParquetHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        engine.process_parquet_stream(file_path, batch_size)
    }
}

/// CSV Streaming In-Memory Reader with Schema Inference
pub struct CsvHandler;

impl FormatHandler for CsvHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        engine.process_delimited_csv(file_path, batch_size, b',')
    }
}
