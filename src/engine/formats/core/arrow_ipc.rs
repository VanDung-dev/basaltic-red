use arrow::array::RecordBatch;
use arrow_ipc::reader::FileReader as ArrowFileReader;
use std::fs::File;

use crate::engine::formats::{read_range_from_source, FormatHandler, OpenedSource};
use crate::error::BazanError;

fn open_ipc_reader(
    file_path: &str,
    columns: &[String],
) -> Result<ArrowFileReader<File>, BazanError> {
    if columns.is_empty() {
        return Ok(ArrowFileReader::try_new(File::open(file_path)?, None)?);
    }

    let schema = ArrowFileReader::try_new(File::open(file_path)?, None)?.schema();
    let mut projection = Vec::new();
    for name in columns {
        let index = schema
            .index_of(name)
            .map_err(|_| BazanError::Message(format!("Column '{}' not found in schema", name)))?;
        if !projection.contains(&index) {
            projection.push(index);
        }
    }

    Ok(ArrowFileReader::try_new(
        File::open(file_path)?,
        Some(projection),
    )?)
}

/// Arrow IPC / Feather Streaming Reader (Tier 1 Core Standard)
#[derive(Debug, Clone, Copy, Default)]
pub struct FeatherHandler;

/// Read a row range starting at an Arrow IPC / Feather RecordBatch ordinal.
pub fn read_arrow_ipc_range(
    file_path: &str,
    batch_ordinal: usize,
    offset: usize,
    limit: usize,
) -> Result<RecordBatch, BazanError> {
    read_arrow_ipc_range_columns(file_path, batch_ordinal, offset, limit, &[])
}

pub fn read_arrow_ipc_range_columns(
    file_path: &str,
    batch_ordinal: usize,
    offset: usize,
    limit: usize,
    columns: &[String],
) -> Result<RecordBatch, BazanError> {
    let mut reader = open_ipc_reader(file_path, columns)?;
    let schema = reader.schema().clone();
    reader.set_index(batch_ordinal)?;

    read_range_from_source(
        OpenedSource {
            schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        },
        offset,
        limit,
    )
}

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

    fn open_with_columns(
        &self,
        file_path: &str,
        _batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        let reader = open_ipc_reader(file_path, columns)?;
        let schema = reader.schema().clone();

        Ok(OpenedSource {
            schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}
