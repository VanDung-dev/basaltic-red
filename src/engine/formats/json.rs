use super::{clamp_batch_size, FormatHandler};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

/// Formatted Pretty Printed JSON Array Reader (Multi-line formatted JSON [ {\n  "id": 1 ... \n} ])
pub struct JsonHandler;

impl FormatHandler for JsonHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        // First attempt native Arrow JSON reader
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);

        if let Ok((schema, _)) = arrow_json::reader::infer_json_schema(&mut buf_reader, Some(100)) {
            let file_for_reader = File::open(file_path)?;
            let buf_reader_2 = BufReader::new(file_for_reader);

            if let Ok(reader) = arrow_json::ReaderBuilder::new(Arc::new(schema))
                .with_batch_size(batch_size)
                .build(buf_reader_2)
            {
                return engine.process_reader(reader);
            }
        }

        // Fallback: Read JSON array using serde_json Value stream for multi-line formatted JSON
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let json_val: serde_json::Value = serde_json::from_reader(reader)?;

        if let serde_json::Value::Array(arr) = json_val {
            let total_rows = arr.len();
            if total_rows == 0 {
                return Ok((0, 0, 0));
            }

            // Convert JSON array into newline-delimited stream cursor.
            // No with_capacity pre-allocation: sizing off total_rows (~250x) let a
            // crafted array force a huge allocation. String grows naturally.
            let mut ndjson_buf = String::new();
            for item in &arr {
                ndjson_buf.push_str(&item.to_string());
                ndjson_buf.push('\n');
            }

            let mut cursor = std::io::Cursor::new(ndjson_buf.as_bytes());
            let schema = arrow_json::reader::infer_json_schema_from_iterator(
                arrow_json::reader::ValueIter::new(&mut cursor, Some(100)),
            )?;

            let cursor_reader = std::io::Cursor::new(ndjson_buf.as_bytes());
            let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                .with_batch_size(batch_size)
                .build(cursor_reader)?;

            engine.process_reader(reader)
        } else {
            Ok((0, 0, 0))
        }
    }
}
