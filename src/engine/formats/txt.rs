use crate::engine::MatrixEngine;
use super::FormatHandler;

/// TXT Streaming In-Memory Reader (Semicolon-Separated Values)
pub struct TxtHandler;

impl FormatHandler for TxtHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), anyhow::Error> {
        engine.process_delimited_csv(file_path, batch_size, b';')
    }
}
