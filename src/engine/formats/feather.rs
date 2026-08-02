use pyo3::prelude::*;
use std::fs::File;
use arrow_ipc::reader::FileReader as ArrowFileReader;
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// Arrow IPC / Feather Streaming Reader
    pub fn process_feather_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        _batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            let file = File::open(&path)?;
            let reader = ArrowFileReader::try_new(file, None)?;

            self.process_reader(reader)
        });

        match stats {
            Ok(res) => Ok(res),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
        }
    }
}
