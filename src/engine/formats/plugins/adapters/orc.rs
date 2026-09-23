use arrow_array::RecordBatchReader;
use orc_rust::ArrowReaderBuilder;
use std::fs::{metadata, File};

use crate::engine::formats::{
    clamp_batch_size, read_range_from_source, FormatHandler, OpenedSource,
};
use crate::error::BazanError;

pub fn read_orc_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<arrow::array::RecordBatch, BazanError> {
    let file_size = usize::try_from(metadata(file_path)?.len())
        .map_err(|_| BazanError::Message("ORC file is too large for this platform".to_string()))?;
    let byte_offset = usize::try_from(byte_offset)
        .map_err(|_| BazanError::Message("ORC stripe offset is too large".to_string()))?
        .min(file_size);
    let file = File::open(file_path)?;
    let builder = ArrowReaderBuilder::try_new(file)
        .map_err(|e| BazanError::Message(format!("ORC error: {e}")))?;
    let schema = builder.schema();
    let reader = builder
        .with_batch_size(clamp_batch_size(batch_size))
        .with_file_byte_range(byte_offset..file_size)
        .build();

    read_range_from_source(
        OpenedSource {
            schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        },
        offset,
        limit,
    )
}

/// Apache ORC Columnar Streaming Reader (Tier 3 Adapter)
#[derive(Debug, Clone, Copy, Default)]
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
