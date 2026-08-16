use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;
use tempfile::tempdir;

use basaltic_red::engine::formats::{resolve_handler_for_file, sniff_format_from_bytes};
use basaltic_red::engine::MatrixEngine;

fn make_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("val", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Float64Array::from(vec![10.5, 20.0, 35.75])),
        ],
    )
    .unwrap()
}

#[test]
fn test_magic_bytes_sniff_unit() {
    assert_eq!(sniff_format_from_bytes(b"PAR1\x00\x00"), Some("parquet"));
    assert_eq!(sniff_format_from_bytes(b"ARROW1\x00"), Some("feather"));
    assert_eq!(sniff_format_from_bytes(b"PK\x03\x04\x14\x00"), Some("xlsx"));
    assert_eq!(sniff_format_from_bytes(b"Obj\x01\x04"), Some("avro"));
    assert_eq!(sniff_format_from_bytes(b"ORC\x01"), Some("orc"));
    assert_eq!(sniff_format_from_bytes(b"  [{\"a\": 1}]"), Some("json"));
    assert_eq!(sniff_format_from_bytes(b"  {\"a\": 1}\n{\"a\": 2}"), Some("ndjson"));
    assert_eq!(sniff_format_from_bytes(b"id,name,val\n1,alice,10"), Some("csv"));
    assert_eq!(sniff_format_from_bytes(b"id\tname\tval\n1\talice\t10"), Some("tsv"));
    assert_eq!(sniff_format_from_bytes(b"id|name|val\n1|alice|10"), Some("psv"));
    assert_eq!(sniff_format_from_bytes(b"id;name;val\n1;alice;10"), Some("txt"));
}

#[test]
fn test_sniff_file_without_extension_parquet() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("data_without_ext");

    // Write Parquet file to a path with NO extension
    let batch = make_test_batch();
    let file = File::create(&file_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let path_str = file_path.to_str().unwrap();

    // Verify resolve_handler_for_file
    let handler = resolve_handler_for_file(path_str);
    assert!(handler.is_some(), "Should sniff Parquet handler without extension");

    // Test slice_rows_native
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let sliced = engine.slice_rows_native(path_str, 0, 10).unwrap();
    assert_eq!(sliced.num_rows(), 3);
    assert_eq!(sliced.num_columns(), 3);
}

#[test]
fn test_sniff_file_without_extension_csv() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("sales_dump_no_ext");

    std::fs::write(
        &file_path,
        "passenger_count,fare_amount,trip_distance\n1,15.5,2.5\n2,25.0,5.0\n",
    )
    .unwrap();

    let path_str = file_path.to_str().unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let sliced = engine.slice_rows_native(path_str, 0, 10).unwrap();
    assert_eq!(sliced.num_rows(), 2);
}

#[test]
fn test_sniff_file_without_extension_json() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("events_no_ext");

    std::fs::write(
        &file_path,
        "[{\"user_id\":101,\"action\":\"click\"},{\"user_id\":102,\"action\":\"buy\"}]",
    )
    .unwrap();

    let path_str = file_path.to_str().unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let sliced = engine.slice_rows_native(path_str, 0, 10).unwrap();
    assert_eq!(sliced.num_rows(), 2);
    assert_eq!(sliced.num_columns(), 2);
}

#[tokio::test]
async fn test_sql_on_file_without_extension() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("raw_parquet_blob");

    let batch = make_test_batch();
    let file = File::create(&file_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let path_str = file_path.to_str().unwrap();

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let sql_query = format!("SELECT name, val FROM '{}' WHERE val > 15.0", path_str);
    let results = engine.execute_sql(&sql_query).await.unwrap();
    assert_eq!(results.num_rows(), 2);
}
