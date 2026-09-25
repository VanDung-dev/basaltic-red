use std::fs::File;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::writer::FileWriter;
use basaltic_red::engine::map::load_lake_map_ipc;
use tempfile::tempdir;

#[test]
fn malformed_map_ipc_with_four_valid_prefix_columns_returns_error_without_panicking() {
    let dir = tempdir().unwrap();
    let map_path = dir.path().join(".br_map.bazan");
    let schema = Arc::new(Schema::new(vec![
        Field::new("rel_path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("mtime_ms", DataType::Int64, false),
        Field::new("total_rows", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(UInt64Array::from(vec![128])),
            Arc::new(Int64Array::from(vec![1_000])),
            Arc::new(UInt64Array::from(vec![10])),
        ],
    )
    .unwrap();

    let mut writer = FileWriter::try_new(File::create(&map_path).unwrap(), &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let result = std::panic::catch_unwind(|| load_lake_map_ipc(&map_path));
    assert!(
        result.is_ok(),
        "map loader panicked on a short, valid IPC schema"
    );
    assert!(
        result.unwrap().is_err(),
        "malformed map schema was accepted"
    );
}
