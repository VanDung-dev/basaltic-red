use super::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::error::BazanError;
use arrow_schema::Schema;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

/// Formatted Pretty Printed JSON Array Reader (Multi-line formatted JSON [ {\n  "id": 1 ... \n} ])
pub struct JsonHandler;

impl FormatHandler for JsonHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        // First attempt native Arrow JSON reader
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);

        if let Ok((schema, _)) = arrow_json::reader::infer_json_schema(&mut buf_reader, Some(100)) {
            let file_for_reader = File::open(file_path)?;
            let buf_reader_2 = BufReader::new(file_for_reader);

            if let Ok(reader) = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
                .with_batch_size(batch_size)
                .build(buf_reader_2)
            {
                return Ok(OpenedSource {
                    schema: Arc::new(schema),
                    batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
                });
            }
        }

        // Fallback: Read JSON array using serde_json Value stream for multi-line formatted JSON
        open_json_array(file_path, batch_size)
    }
}

/// Open a JSON array (`[{...},{...}]`, compact or multi-line) as an NDJSON
/// cursor stream. No with_capacity pre-allocation: sizing off total_rows
/// (~250x) let a crafted array force a huge allocation. String grows naturally.
pub(crate) fn open_json_array(
    file_path: &str,
    batch_size: usize,
) -> Result<OpenedSource, BazanError> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let json_val: serde_json::Value = serde_json::from_reader(reader)?;

    let arr = match json_val {
        serde_json::Value::Array(arr) => arr,
        _ => {
            return Ok(OpenedSource {
                schema: Arc::new(Schema::empty()),
                batches: Box::new(std::iter::empty()),
            })
        }
    };

    if arr.is_empty() {
        return Ok(OpenedSource {
            schema: Arc::new(Schema::empty()),
            batches: Box::new(std::iter::empty()),
        });
    }

    // Convert JSON array into newline-delimited stream cursor.
    let mut ndjson_buf = String::new();
    for item in &arr {
        ndjson_buf.push_str(&item.to_string());
        ndjson_buf.push('\n');
    }

    let mut cursor = std::io::Cursor::new(ndjson_buf.as_bytes());
    let schema = arrow_json::reader::infer_json_schema_from_iterator(
        arrow_json::reader::ValueIter::new(&mut cursor, Some(100)),
    )?;

    let cursor_reader = std::io::Cursor::new(ndjson_buf.into_bytes());
    let reader = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(clamp_batch_size(batch_size))
        .build(cursor_reader)?;

    Ok(OpenedSource {
        schema: Arc::new(schema),
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}
