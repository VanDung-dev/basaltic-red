use super::{FormatHandler, OpenedSource};
use crate::error::BazanError;
use arrow_ipc::reader::FileReader as ArrowFileReader;
use std::fs::File;

/// Arrow IPC / Feather Streaming Reader
pub struct FeatherHandler;

impl FormatHandler for FeatherHandler {
    fn open(&self, file_path: &str, _batch_size: usize) -> Result<OpenedSource, BazanError> {
        let file = File::open(file_path)?;
        let reader = ArrowFileReader::try_new(file, None)?;
        let schema = reader.schema().clone();

        Ok(OpenedSource {
            schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}
