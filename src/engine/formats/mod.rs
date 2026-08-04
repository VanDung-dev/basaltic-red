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

use crate::engine::slice::DEFAULT_MAX_BATCH_SIZE;
use crate::engine::MatrixEngine;
use crate::error::BazanError;

/// Cap a user-supplied batch_size before it reaches arrow readers or
/// `Vec::with_capacity`, which size allocations off it (batch_size = 10^12
/// previously forced a ~24GB allocation). See slice.rs for the same pattern.
pub(crate) fn clamp_batch_size(batch_size: usize) -> usize {
    batch_size.min(DEFAULT_MAX_BATCH_SIZE)
}

/// Pure-Rust streaming reader + audit filter for one file format.
///
/// Each handler owns its format-specific parsing (schema inference, row
/// conversion) and pushes `RecordBatch` through the engine's shared filter
/// loop via `MatrixEngine::process_reader`. No pyo3 here — reusable by the
/// DataFusion provider (GĐ6) and by tests.
pub trait FormatHandler: Sync {
    /// Filter `file_path`, returning (total_rows, clean_rows, trash_rows).
    fn process_file(
        &self,
        engine: &MatrixEngine,
        file_path: &str,
        batch_size: usize,
    ) -> Result<(usize, usize, usize), BazanError>;
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
