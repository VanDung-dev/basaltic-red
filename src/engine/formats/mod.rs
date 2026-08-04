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

use crate::engine::MatrixEngine;

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
    ) -> Result<(usize, usize, usize), anyhow::Error>;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_registry_dispatch_filters_csv() {
        let dir = tempdir().unwrap();
        let csv_path = dir.path().join("data.csv");
        fs::write(
            &csv_path,
            "passenger_count,fare_amount,trip_distance\n\
             1,15.5,2.5\n\
             2,-5.0,0.0\n\
             0,20.0,3.1\n\
             5,100.0,10.0\n\
             12,50.0,1.2\n\
             1,0.0,5.0\n",
        )
        .unwrap();

        let handler = handler_for("csv").unwrap();
        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let (total, clean, trash) = handler
            .process_file(&engine, csv_path.to_str().unwrap(), 1024)
            .unwrap();

        assert_eq!(total, 6);
        assert_eq!(clean, 2);
        assert_eq!(trash, 4);
        assert!(handler_for("nope").is_none());
    }

    #[test]
    fn test_delimited_helper_via_txt_handler() {
        let dir = tempdir().unwrap();
        let txt_path = dir.path().join("data.txt");
        fs::write(
            &txt_path,
            "passenger_count;fare_amount;trip_distance\n\
             1;15.5;2.5\n\
             2;-5.0;0.0\n\
             0;20.0;3.1\n\
             5;100.0;10.0\n\
             12;50.0;1.2\n\
             1;0.0;5.0\n",
        )
        .unwrap();

        let handler = handler_for("txt").unwrap();
        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let (total, clean, trash) = handler
            .process_file(&engine, txt_path.to_str().unwrap(), 1024)
            .unwrap();

        assert_eq!(total, 6);
        assert_eq!(clean, 2);
        assert_eq!(trash, 4);
    }
}
