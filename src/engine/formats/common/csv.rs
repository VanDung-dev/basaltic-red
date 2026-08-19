use std::fs::File;
use std::io::{BufRead, BufReader, Seek};
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema};
use regex::Regex;

use crate::engine::formats::{clamp_batch_size, FormatHandler, OpenedSource};
use crate::engine::MatrixEngine;
use crate::error::BazanError;

static TSV_NULL_REGEX: OnceLock<Regex> = OnceLock::new();

fn tsv_null_regex() -> &'static Regex {
    TSV_NULL_REGEX.get_or_init(|| Regex::new(r"^\\N$").expect("valid regex"))
}

impl MatrixEngine {
    /// Helper method to iterate through RecordBatch reader and sum filter statistics
    pub(crate) fn process_reader<I, E>(
        &self,
        reader: I,
    ) -> Result<(usize, usize, usize), BazanError>
    where
        I: IntoIterator<Item = Result<arrow::array::RecordBatch, E>>,
        BazanError: From<E>,
    {
        let mut total_rows = 0;
        let mut total_clean = 0;
        let mut total_trash = 0;

        for batch_result in reader {
            let batch = batch_result?;
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;

            let (clean_b, trash_b) = self.filter_batch_native(&batch, batch_rows);
            total_clean += clean_b.num_rows();
            total_trash += trash_b.num_rows();
        }

        Ok((total_rows, total_clean, total_trash))
    }
}

/// Generic delimited reader with automatic schema inference
pub fn open_delimited_csv(
    file_path: &str,
    batch_size: usize,
    delimiter: u8,
) -> Result<OpenedSource, BazanError> {
    let mut file = File::open(file_path)?;
    let format = arrow_csv::reader::Format::default()
        .with_delimiter(delimiter)
        .with_header(true);

    let (schema, _) = format.infer_schema(&mut file, Some(100))?;
    let _ = file.rewind();

    let batch_size = clamp_batch_size(batch_size);
    let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_delimiter(delimiter)
        .with_header(true)
        .with_batch_size(batch_size)
        .build(file)?;

    Ok(OpenedSource {
        schema: Arc::new(schema),
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

/// Delimited CSV opener with column projection (arrow-csv `with_projection`).
pub fn open_delimited_csv_columns(
    file_path: &str,
    batch_size: usize,
    delimiter: u8,
    columns: &[String],
) -> Result<OpenedSource, BazanError> {
    let mut file = File::open(file_path)?;
    let format = arrow_csv::reader::Format::default()
        .with_delimiter(delimiter)
        .with_header(true);

    let (schema, _) = format.infer_schema(&mut file, Some(100))?;
    let _ = file.rewind();

    let mut indices = Vec::new();
    for name in columns {
        indices.push(
            schema
                .index_of(name)
                .map_err(|_| BazanError::Message(format!("Column '{}' not found in schema", name)))?,
        );
    }

    let batch_size = clamp_batch_size(batch_size);
    let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_delimiter(delimiter)
        .with_header(true)
        .with_batch_size(batch_size)
        .with_projection(indices)
        .build(file)?;

    Ok(OpenedSource {
        schema: Arc::new(schema),
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

/// CSV Streaming In-Memory Reader with Schema Inference (Tier 2 Common)
#[derive(Debug, Clone, Copy, Default)]
pub struct CsvHandler;

impl FormatHandler for CsvHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_delimited_csv(file_path, batch_size, b',')
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        open_delimited_csv_columns(file_path, batch_size, b',', columns)
    }
}

/// TSV Streaming In-Memory Reader (Tab-Separated Values)
#[derive(Debug, Clone, Copy, Default)]
pub struct TsvHandler;

impl FormatHandler for TsvHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let batch_size = clamp_batch_size(batch_size);
        let mut file = File::open(file_path)?;
        let mut header_line = String::new();
        {
            let mut header_reader = BufReader::new(&mut file);
            header_reader.read_line(&mut header_line)?;
        }
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

        let _ = file.rewind();
        let reader = arrow_csv::ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_delimiter(b'\t')
            .with_null_regex(tsv_null_regex().clone())
            .with_truncated_rows(true)
            .with_batch_size(batch_size)
            .build(file)?;

        Ok(OpenedSource {
            schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}

/// PSV Streaming In-Memory Reader (Pipe-Separated Values)
#[derive(Debug, Clone, Copy, Default)]
pub struct PsvHandler;

impl FormatHandler for PsvHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_delimited_csv(file_path, batch_size, b'|')
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        open_delimited_csv_columns(file_path, batch_size, b'|', columns)
    }
}

/// TXT Streaming In-Memory Reader (Semicolon-Separated Values)
#[derive(Debug, Clone, Copy, Default)]
pub struct TxtHandler;

impl FormatHandler for TxtHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        open_delimited_csv(file_path, batch_size, b';')
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        open_delimited_csv_columns(file_path, batch_size, b';', columns)
    }
}
