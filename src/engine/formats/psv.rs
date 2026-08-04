use super::FormatHandler;
use crate::engine::MatrixEngine;
use crate::error::BazanError;

/// PSV Streaming In-Memory Reader (Pipe-Separated Values)
pub struct PsvHandler;

impl FormatHandler for PsvHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        engine.process_delimited_csv(file_path, batch_size, b'|')
    }
}
