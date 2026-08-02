use pyo3::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// JSONL Single-Line Compact JSON Array Reader ([{"id":1,...},{"id":2,...}])
    pub fn process_jsonl_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
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
                    arrow_json::reader::ValueIter::new(&mut cursor, Some(100))
                )?;

                let cursor_reader = std::io::Cursor::new(ndjson_buf.as_bytes());
                let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                    .with_batch_size(batch_size)
                    .build(cursor_reader)?;

                self.process_reader(reader)
            } else {
                Ok((0, 0, 0))
            }
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
