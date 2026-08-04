use super::FormatHandler;
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

/// JSONL Single-Line Compact JSON Array Reader ([{"id":1,...},{"id":2,...}])
pub struct JsonlHandler;

impl FormatHandler for JsonlHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let json_val: serde_json::Value = serde_json::from_reader(reader)?;

        if let serde_json::Value::Array(arr) = json_val {
            let total_rows = arr.len();
            if total_rows == 0 {
                return Ok((0, 0, 0));
            }

            let mut ndjson_buf = String::with_capacity(total_rows * 250);
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
