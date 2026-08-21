pub mod common;
pub mod core;
pub mod plugins;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::engine::MatrixEngine;
use crate::error::BazanError;

/// Unified 2 GB RAM Budget Max Batch Size: 2^20 = 1,048,576 rows per batch (~500MB-1.5GB RAM)
pub const DEFAULT_MAX_BATCH_SIZE: usize = 1 << 20;

// Re-export all handlers across the 3 Tiers for seamless ergonomics
pub use self::common::*;
pub use self::core::*;
pub use self::plugins::*;

/// Cap a user-supplied batch_size before it reaches arrow readers or
/// `Vec::with_capacity`, which size allocations off it.
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

/// Static Built-in Table mapping default extensions to handlers
static HANDLERS: &[(&str, &dyn FormatHandler)] = &[
    // Tier 1: Core Standard
    ("parquet", &ParquetHandler),
    ("pq", &ParquetHandler),
    ("feather", &FeatherHandler),
    ("arrow", &FeatherHandler),
    ("ipc", &FeatherHandler),
    // Tier 2: Common Built-in
    ("csv", &CsvHandler),
    ("tsv", &TsvHandler),
    ("psv", &PsvHandler),
    ("txt", &TxtHandler),
    ("json", &JsonHandler),
    ("jsonl", &JsonlHandler),
    ("ndjson", &NdjsonHandler),
    // Tier 3: Pluggable Adapters
    ("xlsx", &XlsxHandler),
    ("avro", &AvroHandler),
    ("orc", &OrcHandler),
    ("msgpack", &MsgpackHandler),
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

static DYNAMIC_HANDLERS: OnceLock<RwLock<HashMap<String, Arc<dyn FormatHandler>>>> = OnceLock::new();

fn dynamic_registry() -> &'static RwLock<HashMap<String, Arc<dyn FormatHandler>>> {
    DYNAMIC_HANDLERS.get_or_init(|| RwLock::new(HashMap::new()))
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

/// Inspect raw header bytes to sniff the underlying format.
pub fn sniff_format_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.is_empty() {
        return None;
    }

    // 1. Tier 1 Core: Parquet (b"PAR1")
    if bytes.starts_with(b"PAR1") {
        return Some("parquet");
    }

    // 2. Tier 1 Core: Arrow IPC / Feather (b"ARROW1")
    if bytes.starts_with(b"ARROW1") {
        return Some("feather");
    }

    // 3. Tier 3: Excel XLSX (PKZip header: b"PK\x03\x04")
    if bytes.starts_with(b"PK\x03\x04") {
        return Some("xlsx");
    }

    // 4. Tier 3: Apache Avro (b"Obj\x01")
    if bytes.starts_with(b"Obj\x01") {
        return Some("avro");
    }

    // 5. Tier 3: Apache ORC (b"ORC")
    if bytes.starts_with(b"ORC") {
        return Some("orc");
    }

    // 6. Tier 3: MessagePack (Map / FixMap indicators)
    if matches!(bytes[0], 0x80..=0x8f | 0xde | 0xdf) {
        return Some("msgpack");
    }

    // 7. Tier 2: JSON (Array `[` or Object `{`, skipping leading ASCII whitespace)
    let trimmed = bytes
        .iter()
        .copied()
        .find(|&b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'));
    if let Some(b) = trimmed {
        if b == b'[' {
            return Some("json");
        }
        if b == b'{' {
            return Some("ndjson");
        }
    }

    // 8. Tier 2: CSV / Delimited plaintext sniff
    // Check if the initial chunk is valid UTF-8 text
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Some(first_line) = text.lines().next() {
            if first_line.contains('\t') {
                return Some("tsv");
            } else if first_line.contains('|') {
                return Some("psv");
            } else if first_line.contains(';') {
                return Some("txt");
            } else if first_line.contains(',') {
                return Some("csv");
            }
        }
    }

    None
}

/// Open file, read first 512 bytes, and sniff the format handler.
pub fn sniff_format_from_file(file_path: &str) -> Option<Arc<dyn FormatHandler>> {
    use std::io::Read;
    let mut file = std::fs::File::open(file_path).ok()?;
    let mut buf = [0u8; 512];
    let n = file.read(&mut buf).ok()?;
    let ext = sniff_format_from_bytes(&buf[..n])?;
    handler_for(ext)
}

/// Resolve handler for a file path:
/// 1. Try file extension (fast O(1))
/// 2. If extension is missing or unknown, sniff header magic bytes
pub fn resolve_handler_for_file(file_path: &str) -> Option<Arc<dyn FormatHandler>> {
    let path = std::path::Path::new(file_path);
    if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
        if let Some(h) = handler_for(ext) {
            return Some(h);
        }
    }
    sniff_format_from_file(file_path)
}

/// Print a one-time hint to stderr when a non-parquet file is about to be read.
pub fn maybe_hint_not_parquet(file_path: &str, ext: &str) {
    if matches!(ext, "parquet" | "pq") {
        return;
    }
    use std::collections::HashSet;
    use std::sync::Mutex;
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
