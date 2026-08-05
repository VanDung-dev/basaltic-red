use super::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::error::BazanError;
use arrow_array::RecordBatchReader;
use orc_rust::ArrowReaderBuilder;
use std::fs::File;

/// Apache ORC Columnar Streaming Reader (pure-Rust `orc-rust`, arrow-native).
pub struct OrcHandler;

impl FormatHandler for OrcHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let file = File::open(file_path)?;
        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| BazanError::Message(format!("ORC error: {e}")))?
            .with_batch_size(clamp_batch_size(batch_size))
            .build();
        let schema = reader.schema();
        Ok(OpenedSource {
            schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}
