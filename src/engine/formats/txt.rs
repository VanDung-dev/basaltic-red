use std::fs::File;
use std::sync::Arc;
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
        let file = File::open(file_path)?;
        let format = arrow_csv::reader::Format::default()
            .with_delimiter(b';')
            .with_header(true);

        let (schema, _) = format.infer_schema(file, Some(100))?;

        let file_for_reader = File::open(file_path)?;
        let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_delimiter(b';')
            .with_header(true)
            .with_batch_size(batch_size)
            .build(file_for_reader)?;

        engine.process_reader(reader)
    }
}
