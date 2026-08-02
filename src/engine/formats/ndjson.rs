use pyo3::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// NDJSON Newline Delimited Stream Reader (1 complete JSON object per line, no outer array brackets)
    pub fn process_ndjson_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
            let mut buf_reader = BufReader::new(file);

            let schema = arrow_json::reader::infer_json_schema_from_iterator(
                arrow_json::reader::ValueIter::new(&mut buf_reader, Some(100))
            )?;

            let file_for_reader = File::open(&path)?;
            let buf_reader_2 = BufReader::new(file_for_reader);

            let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                .with_batch_size(batch_size)
                .build(buf_reader_2)?;

            self.process_reader(reader)
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
