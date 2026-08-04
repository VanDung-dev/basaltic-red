use crate::engine::MatrixEngine;
use super::FormatHandler;

/// PSV Streaming In-Memory Reader (Pipe-Separated Values)
pub struct PsvHandler;

impl FormatHandler for PsvHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), anyhow::Error> {
        engine.process_delimited_csv(file_path, batch_size, b'|')
    }
}
