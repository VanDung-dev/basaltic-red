use super::FormatHandler;
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

/// NDJSON Newline Delimited Stream Reader (1 complete JSON object per line, no outer array brackets)
pub struct NdjsonHandler;

impl FormatHandler for NdjsonHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);

        let schema = arrow_json::reader::infer_json_schema_from_iterator(
            arrow_json::reader::ValueIter::new(&mut buf_reader, Some(100)),
        )?;

        let file_for_reader = File::open(file_path)?;
        let buf_reader_2 = BufReader::new(file_for_reader);

        let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(batch_size)
            .build(buf_reader_2)?;

        engine.process_reader(reader)
    }
}
