pub mod avro;
pub mod csv;
pub mod feather;
pub mod json;
pub mod jsonl;
pub mod msgpack;
pub mod ndjson;
pub mod orc;
pub mod parquet;
pub mod psv;
pub mod tsv;
pub mod txt;
pub mod xlsx;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use std::sync::Arc;

use crate::engine::slice::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::MatrixEngine;
use crate::error::BazanError;

/// Cap a user-supplied batch_size before it reaches arrow readers or
/// `Vec::with_capacity`, which size allocations off it (batch_size = 10^12
/// previously forced a ~24GB allocation). See slice.rs for the same pattern.
pub(crate) fn clamp_batch_size(batch_size: usize) -> usize {
    batch_size.min(DEFAULT_MAX_BATCH_SIZE)
}

/// A single source file opened as a schema + stream of RecordBatches.
///
/// This is the ONE read pipeline for every consumer: `process_file` (filter
/// counts), `slice_rows`/`slice_cols`, `filter_files_parallel` and
/// `execute_sql` all go through `FormatHandler::open`.
pub struct OpenedSource {
    pub schema: Arc<Schema>,
    pub batches: Box<dyn Iterator<Item = Result<RecordBatch, BazanError>> + Send>,
}

/// Pure-Rust streaming reader for one file format.
///
/// Each handler owns its format-specific parsing (schema inference, row
/// conversion) and exposes it as `open()`; the shared `process_file` /
/// `read_range` defaults build on top so all consumers behave identically.
pub trait FormatHandler: Sync {
    /// Open `file_path` as a stream of RecordBatches (clamped `batch_size`).
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError>;

    /// Filter `file_path`, returning (total_rows, clean_rows, trash_rows).
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        let source = self.open(file_path, batch_size)?;
        engine.process_reader(source.batches)
    }

    /// Read `limit` rows starting at `offset` by streaming and skipping batches.
    fn read_range(
        &self,
        file_path: &str,
        offset: usize,
        limit: usize,
        batch_size: usize,
    ) -> Result<RecordBatch, BazanError> {
        let source = self.open(file_path, batch_size)?;
        let mut accumulated_rows = 0usize;
        let mut got = 0usize;
        let mut matched_batches = Vec::new();

        for batch_res in source.batches {
            let batch = batch_res?;
            let b_len = batch.num_rows();
            if accumulated_rows + b_len > offset {
                let start_in_batch = offset.saturating_sub(accumulated_rows);
                let len_in_batch = limit.saturating_sub(got).min(b_len - start_in_batch);
                if len_in_batch > 0 {
                    matched_batches.push(batch.slice(start_in_batch, len_in_batch));
                    got += len_in_batch;
                }
            }
            accumulated_rows += b_len;
            if got >= limit {
                break;
            }
        }

        if matched_batches.is_empty() {
            Ok(RecordBatch::new_empty(source.schema))
        } else {
            arrow::compute::concat_batches(&matched_batches[0].schema(), &matched_batches)
                .map_err(BazanError::from)
        }
    }
}

/// Lazily chunk a stream of row-values into RecordBatches of at most `batch_size`.
/// Used by row-oriented handlers (avro, msgpack, xlsx) whose readers are not
/// arrow iterators.
pub(crate) struct RowChunker<I, T, F> {
    rows: I,
    buffer: Vec<T>,
    batch_size: usize,
    schema: Arc<Schema>,
    convert: F,
}

impl<I, T, F> RowChunker<I, T, F> {
    pub(crate) fn new(rows: I, batch_size: usize, schema: Arc<Schema>, convert: F) -> Self {
        Self {
            rows,
            buffer: Vec::with_capacity(batch_size.min(1024)),
            batch_size,
            schema,
            convert,
        }
    }
}

impl<I, T, F> Iterator for RowChunker<I, T, F>
where
    I: Iterator<Item = Result<T, BazanError>>,
    F: Fn(&[T], &Arc<Schema>) -> Result<RecordBatch, BazanError>,
{
    type Item = Result<RecordBatch, BazanError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.buffer.len() < self.batch_size {
            match self.rows.next() {
                Some(Ok(row)) => self.buffer.push(row),
                Some(Err(e)) => return Some(Err(e)),
                None => break,
            }
        }
        if self.buffer.is_empty() {
            return None;
        }
        let result = (self.convert)(&self.buffer, &self.schema);
        self.buffer.clear();
        Some(result)
    }
}

/// Registry: lowercase file extension -> format handler.
///
/// Adding a format = new handler struct + one registry entry, nothing else.
static HANDLERS: &[(&str, &dyn FormatHandler)] = &[
    ("csv", &csv::CsvHandler),
    ("tsv", &tsv::TsvHandler),
    ("psv", &psv::PsvHandler),
    ("txt", &txt::TxtHandler),
    ("json", &json::JsonHandler),
    ("jsonl", &jsonl::JsonlHandler),
    ("ndjson", &ndjson::NdjsonHandler),
    ("parquet", &csv::ParquetHandler),
    ("pq", &csv::ParquetHandler),
    ("feather", &feather::FeatherHandler),
    ("arrow", &feather::FeatherHandler),
    ("ipc", &feather::FeatherHandler),
    ("avro", &avro::AvroHandler),
    ("xlsx", &xlsx::XlsxHandler),
    ("orc", &orc::OrcHandler),
    ("msgpack", &msgpack::MsgpackHandler),
];

/// Resolve a handler by lowercase file extension.
pub fn handler_for(ext: &str) -> Option<&'static dyn FormatHandler> {
    HANDLERS.iter().find(|(e, _)| *e == ext).map(|(_, h)| *h)
}
