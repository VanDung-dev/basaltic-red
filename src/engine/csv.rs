use pyo3::prelude::*;
use std::fs::File;
use std::sync::Arc;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// Helper method to iterate through RecordBatch reader and sum filter statistics
    pub(crate) fn process_reader<I, E>(&self, reader: I) -> Result<(usize, usize, usize), anyhow::Error>
    where
        I: IntoIterator<Item = Result<arrow::array::RecordBatch, E>>,
        E: std::error::Error + Send + Sync + 'static,
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

    /// Parquet Streaming In-Memory Reader
    pub fn process_parquet_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_batch_size(batch_size)
                .build()?;
            self.process_reader(reader)
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }

    /// CSV Streaming In-Memory Reader with Schema Inference
    pub fn process_csv_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
            let (schema, _) = arrow_csv::reader::Format::default()
                .with_header(true)
                .infer_schema(file, Some(100))?;

            let file_for_reader = File::open(&path)?;
            let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
                .with_header(true)
                .with_batch_size(batch_size)
                .build(file_for_reader)?;

            self.process_reader(reader)
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
