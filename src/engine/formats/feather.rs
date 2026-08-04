use std::fs::File;
use arrow_ipc::reader::FileReader as ArrowFileReader;
use crate::engine::MatrixEngine;
use super::FormatHandler;

/// Arrow IPC / Feather Streaming Reader
pub struct FeatherHandler;

impl FormatHandler for FeatherHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        _batch_size: usize,
    ) -> Result<(usize, usize, usize), anyhow::Error> {
        let file = File::open(file_path)?;
        let reader = ArrowFileReader::try_new(file, None)?;

        engine.process_reader(reader)
    }
}
