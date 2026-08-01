use pyo3::prelude::*;
use std::fs::File;
use std::sync::Arc;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// PSV Streaming In-Memory Reader (Pipe-Separated Values)
    pub fn process_psv_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
            let format = arrow_csv::reader::Format::default()
                .with_delimiter(b'|')
                .with_header(true);

            let (schema, _) = format.infer_schema(file, Some(100))?;

            let file_for_reader = File::open(&path)?;
            let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
                .with_delimiter(b'|')
                .with_header(true)
                .with_batch_size(batch_size)
                .build(file_for_reader)?;

            self.process_reader(reader)
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
