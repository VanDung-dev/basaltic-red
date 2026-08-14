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
pub trait FormatHandler: Send + Sync {
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
        read_range_from_source(source, offset, limit)
    }

    /// Open with column projection. Default: read all columns (no pushdown);
    /// handlers whose readers support projection (parquet, csv-family) override.
    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        _columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        self.open(file_path, batch_size)
    }

    /// Read `limit` rows starting at `offset`, projecting to `columns`.
    fn read_range_columns(
        &self,
        file_path: &str,
        offset: usize,
        limit: usize,
        batch_size: usize,
        columns: &[String],
    ) -> Result<RecordBatch, BazanError> {
        let source = self.open_with_columns(file_path, batch_size, columns)?;
        read_range_from_source(source, offset, limit)
    }
}

/// Shared stream-and-skip for `read_range` / `read_range_columns`.
fn read_range_from_source(
    source: OpenedSource,
    offset: usize,
    limit: usize,
) -> Result<RecordBatch, BazanError> {
    if limit == 0 {
        return Ok(RecordBatch::new_empty(source.schema));
    }

    let mut skipped = 0usize;
    let mut collected = 0usize;
    let mut matched_batches = Vec::new();

    for batch_res in source.batches {
        let batch = batch_res?;
        let batch_rows = batch.num_rows();

        if skipped + batch_rows <= offset {
            skipped += batch_rows;
            continue;
        }

        let slice_start = offset.saturating_sub(skipped);
        let available_in_batch = batch_rows - slice_start;
        let take = (limit - collected).min(available_in_batch);

        matched_batches.push(batch.slice(slice_start, take));
        collected += take;
        skipped += batch_rows;

        if collected >= limit {
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

/// Base Template for custom delimited formats (e.g. `|`, `~`, `;`, `^`, tab, custom char).
#[derive(Debug, Clone)]
pub struct DelimitedFormatHandler {
    pub delimiter: u8,
    pub has_header: bool,
}

impl DelimitedFormatHandler {
    pub fn new(delimiter: u8, has_header: bool) -> Self {
        Self {
            delimiter,
            has_header,
        }
    }
}

impl FormatHandler for DelimitedFormatHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        let file = std::fs::File::open(file_path)?;
        let format = arrow::csv::reader::Format::default()
            .with_delimiter(self.delimiter)
            .with_header(self.has_header);

        let (schema, _) = format.infer_schema(file, Some(100))?;
        let batch_size = clamp_batch_size(batch_size);
        let file_for_reader = std::fs::File::open(file_path)?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_delimiter(self.delimiter)
            .with_header(self.has_header)
            .with_batch_size(batch_size)
            .build(file_for_reader)?;

        Ok(OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }

    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        let file = std::fs::File::open(file_path)?;
        let format = arrow::csv::reader::Format::default()
            .with_delimiter(self.delimiter)
            .with_header(self.has_header);

        let (schema, _) = format.infer_schema(file, Some(100))?;
        let mut indices = Vec::new();
        for name in columns {
            indices.push(
                schema
                    .index_of(name)
                    .map_err(|_| BazanError::Message(format!("Column '{}' not found in schema", name)))?,
            );
        }

        let batch_size = clamp_batch_size(batch_size);
        let file_for_reader = std::fs::File::open(file_path)?;
        let reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_delimiter(self.delimiter)
            .with_header(self.has_header)
            .with_batch_size(batch_size)
            .with_projection(indices)
            .build(file_for_reader)?;

        Ok(OpenedSource {
            schema: Arc::new(schema),
            batches: Box::new(reader.map(|r| r.map_err(BazanError::from))),
        })
    }
}

/// Static Built-in Table: lowercase file extension -> format handler.
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

struct StaticRefHandler(&'static dyn FormatHandler);

impl FormatHandler for StaticRefHandler {
    fn open(&self, file_path: &str, batch_size: usize) -> Result<OpenedSource, BazanError> {
        self.0.open(file_path, batch_size)
    }
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError> {
        self.0.process_file(engine, file_path, batch_size)
    }
    fn read_range(
        &self,
        file_path: &str,
        offset: usize,
        limit: usize,
        batch_size: usize,
    ) -> Result<RecordBatch, BazanError> {
        self.0.read_range(file_path, offset, limit, batch_size)
    }
    fn open_with_columns(
        &self,
        file_path: &str,
        batch_size: usize,
        columns: &[String],
    ) -> Result<OpenedSource, BazanError> {
        self.0.open_with_columns(file_path, batch_size, columns)
    }
    fn read_range_columns(
        &self,
        file_path: &str,
        offset: usize,
        limit: usize,
        batch_size: usize,
        columns: &[String],
    ) -> Result<RecordBatch, BazanError> {
        self.0.read_range_columns(file_path, offset, limit, batch_size, columns)
    }
}

static DYNAMIC_HANDLERS: std::sync::OnceLock<
    std::sync::RwLock<std::collections::HashMap<String, Arc<dyn FormatHandler>>>,
> = std::sync::OnceLock::new();

fn dynamic_registry(
) -> &'static std::sync::RwLock<std::collections::HashMap<String, Arc<dyn FormatHandler>>> {
    DYNAMIC_HANDLERS.get_or_init(|| std::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Register a custom format handler dynamically at runtime.
pub fn register_format(ext: &str, handler: Arc<dyn FormatHandler>) {
    let mut reg = dynamic_registry().write().unwrap();
    reg.insert(ext.to_lowercase(), handler);
}

/// Unregister a dynamically registered format handler.
pub fn unregister_format(ext: &str) -> bool {
    let mut reg = dynamic_registry().write().unwrap();
    reg.remove(&ext.to_lowercase()).is_some()
}

/// List all currently supported format extensions (both built-in and dynamic).
pub fn list_supported_formats() -> Vec<String> {
    let mut formats: Vec<String> = HANDLERS.iter().map(|(ext, _)| ext.to_string()).collect();
    if let Ok(reg) = dynamic_registry().read() {
        for ext in reg.keys() {
            if !formats.contains(ext) {
                formats.push(ext.clone());
            }
        }
    }
    formats.sort();
    formats
}

/// Resolve a handler by lowercase file extension (Dynamic registry first, fallback to static built-ins).
pub fn handler_for(ext: &str) -> Option<Arc<dyn FormatHandler>> {
    let ext_lower = ext.to_lowercase();
    if let Ok(reg) = dynamic_registry().read() {
        if let Some(h) = reg.get(&ext_lower) {
            return Some(h.clone());
        }
    }
    HANDLERS
        .iter()
        .find(|(e, _)| *e == ext_lower)
        .map(|(_, h)| Arc::new(StaticRefHandler(*h)) as Arc<dyn FormatHandler>)
}

/// Print a one-time hint to stderr when a non-parquet file is about to be read
/// (once per format per process). Parquet gets full parallel read power; other
/// formats stream safely but miss pushdown/row-group parallelism.
pub fn maybe_hint_not_parquet(file_path: &str, ext: &str) {
    if matches!(ext, "parquet" | "pq") {
        return;
    }
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static HINTED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let mut seen = HINTED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap();
    if seen.insert(ext.to_string()) {
        eprintln!(
            "[basaltic-red] hint: '{}' is not parquet — convert with \
             br.lake.ingest('{}', out_dir) for full parallel read power",
            file_path, file_path
        );
    }
}
