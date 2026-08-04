use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::engine::MatrixEngine;
use super::FormatHandler;

/// Apache ORC Columnar Streaming Reader (using Parquet/Arrow Reader interface)
pub struct OrcHandler;

impl FormatHandler for OrcHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), anyhow::Error> {
        let file = File::open(file_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(batch_size)
            .build()?;

        engine.process_reader(reader)
    }
}
