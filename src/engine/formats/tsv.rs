use super::{clamp_batch_size, FormatHandler};
use crate::engine::MatrixEngine;
use crate::error::BazanError;
use arrow_schema::{DataType, Field, Schema};
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

/// TSV Streaming In-Memory Reader (Tab-Separated Values)
pub struct TsvHandler;

impl FormatHandler for TsvHandler {
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let header_file = File::open(file_path)?;
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
        let file_for_reader = File::open(file_path)?;
        let reader = arrow_csv::ReaderBuilder::new(schema)
            .with_header(true)
            .with_delimiter(b'\t')
            .with_null_regex(null_regex)
            .with_truncated_rows(true)
            .with_batch_size(batch_size)
            .build(file_for_reader)?;

        engine.process_reader(reader)
    }
}
