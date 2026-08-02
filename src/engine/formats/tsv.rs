use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use regex::Regex;
use arrow_schema::{DataType, Field, Schema};
use crate::engine::MatrixEngine;

impl MatrixEngine {
    /// TSV Streaming In-Memory Reader (Tab-Separated Values)
    pub fn process_tsv_file(
        &self,
        py: Python<'_>,
        file_path: &str,
        batch_size: usize,
    ) -> PyResult<(usize, usize, usize)> {
        let path = file_path.to_string();

        let stats = py.detach(|| -> Result<(usize, usize, usize), anyhow::Error> {
            // Parse column names from header line
            let header_file = File::open(&path)?;
            let mut header_reader = BufReader::new(header_file);
            let mut header_line = String::new();
            header_reader.read_line(&mut header_line)?;
            let col_names: Vec<String> = header_line
                .trim_end_matches(['\n', '\r'])
                .split('\t')
                .map(|s| s.to_string())
                .collect();

            // Force all columns as Utf8 — safest for raw/dirty TSV data
            let fields: Vec<Field> = col_names
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));

            let null_regex = Regex::new(r"^\\N$")?;
            let file_for_reader = File::open(&path)?;
            let reader = arrow_csv::ReaderBuilder::new(schema)
                .with_header(true)
                .with_delimiter(b'\t')
                .with_null_regex(null_regex)
                .with_truncated_rows(true)
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
