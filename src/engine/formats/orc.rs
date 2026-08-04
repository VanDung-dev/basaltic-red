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
        engine.process_parquet_stream(file_path, batch_size)
    }
}
