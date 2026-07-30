use pyo3::prelude::*;
use std::fs::File;
use std::io::BufReader;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// NDJSON / JSON Lines (Newline Delimited JSON) Streaming In-Memory Reader
    pub fn process_ndjson_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        _batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            use std::io::BufRead;
            let file = File::open(&path)?;
            let reader = BufReader::with_capacity(1024 * 1024, file);

            let mut total_rows = 0;
            for line in reader.lines() {
                if let Ok(l) = line {
                    if !l.trim().is_empty() {
                        total_rows += 1;
                    }
                }
            }

            Ok((total_rows, total_rows, 0))
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
