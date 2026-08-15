use std::sync::Arc;

use crate::engine::formats::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::error::BazanError;

/// Base Template for custom delimited formats (e.g. `|`, `~`, `;`, `^`, tab, custom char).
#[derive(Debug, Clone)]
pub struct DelimitedFormatHandler {
    pub delimiter: u8,
    pub has_header: bool,
}

impl DelimitedFormatHandler {
    pub fn new(delimiter: u8, has_header: bool) -> Self {
        Self {
            delimiter,
            has_header,
        }
    }
}

impl FormatHandler for DelimitedFormatHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let file = std::fs::File::open(file_path)?;
        let format = arrow::csv::reader::Format::default()
            .with_delimiter(self.delimiter)
            .with_header(self.has_header);

        let (schema, _) = format.infer_schema(file, Some(100))?;
        let batch_size = clamp_batch_size(batch_size);
        let file_for_reader = std::fs::File::open(file_path)?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_delimiter(self.delimiter)
            .with_header(self.has_header)
            .with_batch_size(batch_size)
            .build(file_for_reader)?;

        Ok(OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        let file = std::fs::File::open(file_path)?;
        let format = arrow::csv::reader::Format::default()
            .with_delimiter(self.delimiter)
            .with_header(self.has_header);

        let (schema, _) = format.infer_schema(file, Some(100))?;
        let mut indices = Vec::new();
        for name in columns {
            indices.push(
                schema
                    .index_of(name)
                    .map_err(|_| BazanError::Message(format!("Column '{}' not found in schema", name)))?,
            );
        }

        let batch_size = clamp_batch_size(batch_size);
        let file_for_reader = std::fs::File::open(file_path)?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_delimiter(self.delimiter)
            .with_header(self.has_header)
            .with_batch_size(batch_size)
            .with_projection(indices)
            .build(file_for_reader)?;

        Ok(OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}
