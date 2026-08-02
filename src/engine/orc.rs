use pyo3::prelude::*;
use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// Apache ORC Columnar Streaming Reader (using Parquet/Arrow Reader interface)
    pub fn process_orc_file(
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
}
