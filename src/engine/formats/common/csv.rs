use arrow::array::RecordBatch;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema};
use regex::Regex;

use crate::engine::formats::{
    clamp_batch_size, read_range_from_source, FormatHandler, OpenedSource,
};
use crate::engine::MatrixEngine;
use crate::error::BazanError;

static TSV_NULL_REGEX: OnceLock<Regex> = OnceLock::new();

fn tsv_null_regex() -> &'static Regex {
    TSV_NULL_REGEX.get_or_init(|| Regex::new(r"^\\N$").expect("valid regex"))
}

fn infer_tsv_schema(file_path: &str) -> Result<Arc<Schema>, BazanError> {
    let mut file = File::open(file_path)?;
    let mut header_line = String::new();
    BufReader::new(&mut file).read_line(&mut header_line)?;
    let fields: Vec<Field> = header_line
        .trim_end_matches(['\n', '\r'])
        .split('\t')
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    Ok(Arc::new(Schema::new(fields)))
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
fn infer_csv_schema(file_path: &str, delimiter: u8) -> Result<Schema, BazanError> {
    let mut file = File::open(file_path)?;
    let format = arrow_csv::reader::Format::default()
        .with_delimiter(delimiter)
        .with_header(true);
    Ok(format.infer_schema(&mut file, Some(100))?.0)
}

pub(crate) fn build_delimited_source(
    file: File,
    schema: Schema,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
    projection: Option<Vec<usize>>,
) -> Result<OpenedSource, BazanError> {
    let mut builder = arrow_csv::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_delimiter(delimiter)
        .with_header(has_header)
        .with_batch_size(clamp_batch_size(batch_size));
    if let Some(indices) = projection {
        builder = builder.with_projection(indices);
    }
    let reader = builder.build(file)?;

    Ok(OpenedSource {
        schema: Arc::new(schema),
        batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
    })
}

pub fn open_delimited_csv(
    file_path: &str,
    batch_size: usize,
    delimiter: u8,
) -> Result<OpenedSource, BazanError> {
    let schema = infer_csv_schema(file_path, delimiter)?;
    let file = File::open(file_path)?;
    build_delimited_source(file, schema, batch_size, delimiter, true, None)
}

/// Delimited CSV opener with column projection (arrow-csv `with_projection`).
pub fn open_delimited_csv_columns(
    file_path: &str,
    batch_size: usize,
    delimiter: u8,
    columns: &[String],
) -> Result<OpenedSource, BazanError> {
    let schema = infer_csv_schema(file_path, delimiter)?;
    let file = File::open(file_path)?;

    let mut indices = Vec::new();
    for name in columns {
        indices.push(
            schema.index_of(name).map_err(|_| {
                BazanError::Message(format!("Column '{}' not found in schema", name))
            })?,
        );
    }

    build_delimited_source(file, schema, batch_size, delimiter, true, Some(indices))
}

/// Read a CSV row range starting at a quote-safe byte checkpoint.
pub fn read_delimited_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
    delimiter: u8,
    force_utf8: bool,
) -> Result<RecordBatch, BazanError> {
    read_delimited_range_with_columns(
        file_path,
        byte_offset,
        offset,
        limit,
        batch_size,
        delimiter,
        force_utf8,
        &[],
    )
}

/// Read a delimited row range from a byte checkpoint, projecting columns in the parser.
pub fn read_delimited_range_with_columns(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
    delimiter: u8,
    force_utf8: bool,
    columns: &[String],
) -> Result<RecordBatch, BazanError> {
    let schema = if force_utf8 {
        infer_tsv_schema(file_path)?
    } else {
        Arc::new(infer_csv_schema(file_path, delimiter)?)
    };
    let projection = columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| BazanError::Message(format!("Column '{}' not found in schema", name)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let output_schema = if columns.is_empty() {
        schema.clone()
    } else {
        Arc::new(schema.project(&projection)?)
    };
    let mut file = File::open(file_path)?;
    file.seek(SeekFrom::Start(byte_offset))?;
    let batch_size = clamp_batch_size(batch_size);
    let mut builder = arrow_csv::ReaderBuilder::new(schema.clone())
        .with_delimiter(delimiter)
        .with_header(false)
        .with_batch_size(batch_size);
    if !columns.is_empty() {
        builder = builder.with_projection(projection);
    }
    if force_utf8 {
        builder = builder
            .with_null_regex(tsv_null_regex().clone())
            .with_truncated_rows(true);
    }
    let reader = builder.build(file)?;

    read_range_from_source(
        OpenedSource {
            schema: output_schema,
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        },
        offset,
        limit,
    )
}

pub fn read_csv_range(
    file_path: &str,
    byte_offset: u64,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<RecordBatch, BazanError> {
    read_delimited_range(
        file_path,
        byte_offset,
        offset,
        limit,
        batch_size,
        b',',
        false,
    )
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
        let schema = infer_tsv_schema(file_path)?;
        let file = File::open(file_path)?;
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
