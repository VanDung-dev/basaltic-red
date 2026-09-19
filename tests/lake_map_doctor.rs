use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::writer::FileWriter;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use basaltic_red::engine::map::{
    load_lake_map_ipc, resolve_map_path, resolve_parquet_range, RowGroupLocation,
};
use basaltic_red::engine::MatrixEngine;

fn create_sample_parquet(path: &std::path::Path, rows: usize, fare_base: f64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("passenger_count", DataType::Int64, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("trip_distance", DataType::Float64, true),
        Field::new("vendor_id", DataType::Utf8, true),
    ]));

    let passengers: Vec<i64> = (0..rows).map(|i| (i % 6 + 1) as i64).collect();
    let fares: Vec<f64> = (0..rows).map(|i| fare_base + (i % 20) as f64).collect();
    let distances: Vec<f64> = (0..rows).map(|i| 1.5 + (i % 10) as f64).collect();
    let vendors: Vec<&str> = (0..rows)
        .map(|i| if i % 2 == 0 { "VTS" } else { "CMT" })
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(passengers)),
            Arc::new(Float64Array::from(fares)),
            Arc::new(Float64Array::from(distances)),
            Arc::new(StringArray::from(vendors)),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn create_multi_batch_stats_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "fare_amount",
        DataType::Float64,
        false,
    )]));
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer
        .write(
            &RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Float64Array::from(vec![100.0]))],
            )
            .unwrap(),
        )
        .unwrap();
    writer
        .write(
            &RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
}

fn create_multi_row_group_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    writer
        .write(
            &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..10))])
                .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
}

#[test]
fn test_lake_map_creation_and_fast_load() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let lake_root = temp_dir.path();

    let file1 = lake_root.join("year=2026/month=08/part-001.parquet");
    let file2 = lake_root.join("year=2026/month=08/part-002.parquet");

    create_sample_parquet(&file1, 5_000, 10.0);
    create_sample_parquet(&file2, 8_000, 50.0);

    let lake_root_str = lake_root.to_str().unwrap();
    let map_path_str = engine.create_lake_map_native(lake_root_str, false).unwrap();

    let map_file = resolve_map_path(lake_root);
    assert!(map_file.exists());
    assert_eq!(map_path_str, map_file.to_string_lossy().to_string());

    // Measure binary IPC load speed
    let start = Instant::now();
    let map = load_lake_map_ipc(&map_file).unwrap();
    let elapsed = start.elapsed();

    println!("Arrow IPC Map load time: {:?}", elapsed);
    assert!(elapsed.as_millis() < 50, "Map load should be instantaneous");

    assert_eq!(map.total_files, 2);
    assert_eq!(map.total_rows, 13_000);
    assert!(map.total_bytes > 0);
    let metadata = map.to_record_batch().unwrap().schema().metadata().clone();
    assert_eq!(
        metadata.get("bazan.kind").map(String::as_str),
        Some("lake_map")
    );
    assert_eq!(metadata.get("bazan.version").map(String::as_str), Some("1"));

    // Verify relative paths are stored without hardcoded root
    for entry in &map.entries {
        assert!(!entry.rel_path.starts_with('/'));
        assert!(entry.rel_path.contains("part-00"));
        assert!(entry.stats_json.contains("fare_amount"));
    }
}

#[test]
fn test_lake_map_indexes_arrow_ipc_data() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.ipc");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let file = File::create(&data_path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    assert_eq!(map.total_files, 1);
    assert_eq!(map.entries[0].rel_path, "data.ipc");
    assert_eq!(map.total_rows, 3);
}

#[test]
fn test_legacy_map_filename_is_not_loaded() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("data.parquet");
    create_sample_parquet(&file, 3, 10.0);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();
    let current_map = resolve_map_path(temp_dir.path());
    let legacy_map = temp_dir.path().join(".br_map.ipc");
    std::fs::rename(&current_map, &legacy_map).unwrap();

    let error = engine
        .locate_lake_row_native(temp_dir.path().to_str().unwrap(), 2)
        .unwrap_err();
    assert!(!error.to_string().is_empty());

    let report = engine
        .doctor_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();
    assert_eq!(report.status, "DRIFT_DETECTED");
    assert_eq!(report.unindexed_files, vec!["data.parquet"]);
}

#[test]
fn test_doctor_drift_detection_and_auto_heal() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let lake_root = temp_dir.path();

    let file1 = lake_root.join("region=us/part-001.parquet");
    let file2 = lake_root.join("region=eu/part-002.parquet");

    create_sample_parquet(&file1, 1_000, 10.0);
    create_sample_parquet(&file2, 2_000, 20.0);

    let lake_root_str = lake_root.to_str().unwrap();
    engine.create_lake_map_native(lake_root_str, false).unwrap();

    // 1. Initial health check: should be HEALTHY
    let report = engine.doctor_lake_map_native(lake_root_str, false).unwrap();
    assert_eq!(report.status, "HEALTHY");
    assert_eq!(report.healthy_count, 2);
    assert!(report.missing_files.is_empty());
    assert!(report.modified_files.is_empty());
    assert!(report.unindexed_files.is_empty());

    // 2. Introduce drift:
    // a. Add unindexed file
    let file3 = lake_root.join("region=ap/part-003.parquet");
    create_sample_parquet(&file3, 3_000, 30.0);

    // b. Modify existing file
    std::thread::sleep(std::time::Duration::from_millis(20));
    create_sample_parquet(&file1, 1_500, 15.0);

    // c. Delete existing file
    std::fs::remove_file(&file2).unwrap();

    // 3. Run doctor without auto-heal: should report drift accurately
    let drift_report = engine.doctor_lake_map_native(lake_root_str, false).unwrap();
    assert_eq!(drift_report.status, "DRIFT_DETECTED");
    assert_eq!(drift_report.healthy_count, 0);
    assert_eq!(drift_report.unindexed_files.len(), 1);
    assert_eq!(drift_report.modified_files.len(), 1);
    assert_eq!(drift_report.missing_files.len(), 1);
    assert!(!drift_report.healed);

    // 4. Run doctor with auto_heal=true: should repair map incrementally
    let heal_report = engine.doctor_lake_map_native(lake_root_str, true).unwrap();
    assert_eq!(heal_report.status, "HEALED");
    assert!(heal_report.healed);

    // 5. Subsequent check should now be 100% HEALTHY
    let post_heal = engine.doctor_lake_map_native(lake_root_str, false).unwrap();
    assert_eq!(post_heal.status, "HEALTHY");
    assert_eq!(post_heal.healthy_count, 2); // file1 (modified) + file3 (new)
    assert!(post_heal.missing_files.is_empty());
    assert!(post_heal.modified_files.is_empty());
    assert!(post_heal.unindexed_files.is_empty());

    let map = load_lake_map_ipc(&resolve_map_path(lake_root)).unwrap();
    assert_eq!(map.total_files, 2);
    assert_eq!(map.total_rows, 4_500); // 1,500 + 3,000
}

#[test]
fn test_lake_map_stats_cover_all_batches() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("multi_batch.parquet");
    create_multi_batch_stats_parquet(&file);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();
    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let stats: serde_json::Value = serde_json::from_str(&map.entries[0].stats_json).unwrap();

    assert_eq!(stats["columns"]["fare_amount"]["min"].as_f64(), Some(1.0));
    assert_eq!(stats["columns"]["fare_amount"]["max"].as_f64(), Some(100.0));
}

#[test]
fn test_doctor_rejects_corrupt_map() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    std::fs::write(resolve_map_path(temp_dir.path()), b"not an Arrow IPC file").unwrap();

    let error = engine
        .doctor_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap_err();
    assert!(!error.to_string().is_empty());
}

#[test]
fn test_lake_map_resolves_parquet_row_groups_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.parquet");
    create_multi_row_group_parquet(&file);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert!(row_groups.len() >= 3);
    assert_eq!(row_groups[0].first_row, 0);
    assert_eq!(row_groups[0].columns[0].path, "value");
    assert!(!row_groups[0].columns[0].pages.is_empty());

    let location = map.locate_global_row(5).unwrap().unwrap();
    assert_eq!(location.rel_path, "mapped.parquet");
    assert_eq!(location.file_offset, 5);
    assert_eq!(location.row_group, Some(1));
    assert_eq!(location.row_in_group, Some(1));
    assert!(location.page_indexed);

    let resolved = resolve_parquet_range(&file, 5, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 1);
    assert_eq!(resolved.row_groups, vec![1]);

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 5, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), 5);
    assert_eq!(values.value(1), 6);
}
