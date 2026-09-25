use std::fs::File;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow::array::{
    BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_ipc::writer::FileWriter;
use basaltic_red::engine::map::{
    load_lake_map_ipc, resolve_arrow_ipc_range, resolve_map_path, resolve_parquet_range,
};
use basaltic_red::engine::MatrixEngine;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn one_column_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn int_batch(schema: &Arc<Schema>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn write_multi_row_group_parquet(path: &std::path::Path) {
    let schema = one_column_schema();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema.clone(), Some(props)).unwrap();
    writer.write(&int_batch(&schema, (0..8).collect())).unwrap();
    writer.close().unwrap();
}

fn write_empty_parquet(path: &std::path::Path) {
    let schema = one_column_schema();
    ArrowWriter::try_new(File::create(path).unwrap(), schema, None)
        .unwrap()
        .close()
        .unwrap();
}

fn write_arrow_ipc_batches(path: &std::path::Path, batches: &[Vec<i64>]) {
    let schema = one_column_schema();
    let mut writer = FileWriter::try_new(File::create(path).unwrap(), &schema).unwrap();
    for values in batches {
        writer.write(&int_batch(&schema, values.clone())).unwrap();
    }
    writer.finish().unwrap();
}

fn write_empty_arrow_ipc(path: &std::path::Path) {
    write_arrow_ipc_batches(path, &[]);
}

fn write_stats_arrow_ipc(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("enabled", DataType::Boolean, true),
        Field::new("all_null", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("decimal", DataType::Decimal128(10, 2), true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "nested",
            DataType::Struct(vec![Arc::new(Field::new("value", DataType::Int32, true))].into()),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3, 1, 2])),
            Arc::new(Float64Array::from(vec![Some(3.5), None, Some(-1.0)])),
            Arc::new(StringArray::from(vec![Some("z"), Some("a"), None])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])),
            Arc::new(Int64Array::from(vec![None, None, None])),
            Arc::new(UInt64Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(
                Decimal128Array::from(vec![Some(100_i128), Some(200), Some(300)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(TimestampSecondArray::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(StructArray::new(
                vec![Arc::new(Field::new("value", DataType::Int32, true))].into(),
                vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
                None,
            )),
        ],
    )
    .unwrap();
    let mut writer = FileWriter::try_new(File::create(path).unwrap(), &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

fn write_legacy_five_column_map(map_path: &std::path::Path, data_path: &std::path::Path) {
    let metadata = std::fs::metadata(data_path).unwrap();
    let mtime_ms = metadata
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let schema = Arc::new(Schema::new(vec![
        Field::new("rel_path", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("mtime_ms", DataType::Int64, false),
        Field::new("total_rows", DataType::UInt64, false),
        Field::new("stats_json", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["data.parquet"])),
            Arc::new(UInt64Array::from(vec![metadata.len()])),
            Arc::new(Int64Array::from(vec![mtime_ms])),
            Arc::new(UInt64Array::from(vec![8])),
            Arc::new(StringArray::from(vec!["{}"])),
        ],
    )
    .unwrap();
    let mut writer = FileWriter::try_new(File::create(map_path).unwrap(), &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

#[test]
fn parquet_map_slice_crosses_row_groups_and_returns_empty_at_eof() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("data.parquet");
    write_multi_row_group_parquet(&path);
    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let resolved = resolve_parquet_range(&path, 1, 4).unwrap().unwrap();
    assert_eq!(resolved.row_groups, vec![0, 1, 2]);
    assert_eq!(resolved.offset, 1);
    assert_eq!(
        ids(&engine
            .slice_rows_native(path.to_str().unwrap(), 1, 4)
            .unwrap()),
        vec![1, 2, 3, 4]
    );

    let at_eof = engine
        .slice_rows_native(path.to_str().unwrap(), 8, 3)
        .unwrap();
    assert_eq!(at_eof.num_rows(), 0);
    assert_eq!(at_eof.schema().field(0).name(), "id");

    std::fs::remove_file(resolve_map_path(temp_dir.path())).unwrap();
    assert_eq!(
        ids(&engine
            .slice_rows_native(path.to_str().unwrap(), 1, 4)
            .unwrap()),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        engine
            .slice_rows_native(path.to_str().unwrap(), 8, 3)
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn arrow_ipc_map_slice_skips_empty_batch_and_crosses_batch_boundary() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("data.feather");
    write_arrow_ipc_batches(&path, &[vec![0, 1, 2], vec![], vec![3, 4, 5]]);
    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let groups: Vec<basaltic_red::engine::map::RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(groups.len(), 3);
    assert_eq!(groups[1].row_count, 0);
    assert_eq!(groups[2].ordinal, 2);
    let resolved = resolve_arrow_ipc_range(&path, 2, 3).unwrap().unwrap();
    assert_eq!(resolved.batch_ordinal, 0);
    assert_eq!(resolved.offset, 2);
    assert_eq!(
        ids(&engine
            .slice_rows_native(path.to_str().unwrap(), 2, 3)
            .unwrap()),
        vec![2, 3, 4]
    );

    let at_eof = engine
        .slice_rows_native(path.to_str().unwrap(), 6, 2)
        .unwrap();
    assert_eq!(at_eof.num_rows(), 0);
    assert_eq!(at_eof.schema().field(0).name(), "id");

    std::fs::remove_file(resolve_map_path(temp_dir.path())).unwrap();
    assert_eq!(
        ids(&engine
            .slice_rows_native(path.to_str().unwrap(), 2, 3)
            .unwrap()),
        vec![2, 3, 4]
    );
}

#[test]
fn empty_parquet_and_arrow_ipc_files_keep_their_schema_when_sliced() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let parquet_path = temp_dir.path().join("empty.parquet");
    let ipc_path = temp_dir.path().join("empty.arrow");
    write_empty_parquet(&parquet_path);
    write_empty_arrow_ipc(&ipc_path);
    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    for path in [&parquet_path, &ipc_path] {
        let batch = engine
            .slice_rows_native(path.to_str().unwrap(), 0, 10)
            .unwrap();
        assert_eq!(batch.num_rows(), 0, "{}", path.display());
        assert_eq!(batch.schema().field(0).name(), "id", "{}", path.display());
    }
}

#[test]
fn legacy_five_column_map_loads_and_parquet_slice_falls_back() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("data.parquet");
    write_multi_row_group_parquet(&path);
    write_legacy_five_column_map(&resolve_map_path(temp_dir.path()), &path);

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    assert_eq!(map.total_rows, 8);
    assert_eq!(map.entries[0].row_groups_json, "[]");
    let rows = engine
        .slice_rows_native(path.to_str().unwrap(), 3, 3)
        .unwrap();
    assert_eq!(ids(&rows), vec![3, 4, 5]);
}

#[test]
fn lake_stats_cover_only_supported_non_null_arrow_types() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("stats.arrow");
    write_stats_arrow_ipc(&path);
    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let stats: serde_json::Value = serde_json::from_str(&map.entries[0].stats_json).unwrap();
    assert_eq!(stats["total_rows"], 3);
    assert_eq!(stats["columns"]["id"]["min"].as_f64(), Some(1.0));
    assert_eq!(stats["columns"]["id"]["max"].as_f64(), Some(3.0));
    assert_eq!(stats["columns"]["score"]["min"].as_f64(), Some(-1.0));
    assert_eq!(stats["columns"]["score"]["max"].as_f64(), Some(3.5));
    assert_eq!(stats["columns"]["label"]["min_str"], "a");
    assert_eq!(stats["columns"]["label"]["max_str"], "z");
    assert!(stats["columns"].get("enabled").is_none());
    assert!(stats["columns"].get("all_null").is_none());
    assert!(stats["columns"].get("unsigned").is_none());
    assert!(stats["columns"].get("decimal").is_none());
    assert!(stats["columns"].get("timestamp").is_none());
    assert!(stats["columns"].get("nested").is_none());
}

#[test]
fn lake_global_rows_follow_sorted_paths_across_formats() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let arrow_path = temp_dir.path().join("z.arrow");
    let parquet_path = temp_dir.path().join("a.parquet");
    write_arrow_ipc_batches(&arrow_path, &[vec![4, 5, 6]]);
    write_multi_row_group_parquet(&parquet_path);
    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    assert_eq!(map.total_files, 2);
    assert_eq!(map.total_rows, 11);
    assert_eq!(map.entries[0].rel_path, "a.parquet");
    assert_eq!(map.entries[0].first_global_row, 0);
    assert_eq!(map.entries[1].rel_path, "z.arrow");
    assert_eq!(map.entries[1].first_global_row, 8);
    let location = map.locate_global_row(8).unwrap().unwrap();
    assert_eq!(location.rel_path, "z.arrow");
    assert_eq!(location.file_offset, 0);
}
