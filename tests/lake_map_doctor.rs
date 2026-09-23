use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::writer::FileWriter;
use orc_rust::ArrowWriterBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use basaltic_red::engine::map::{
    load_lake_map_ipc, resolve_arrow_ipc_range, resolve_csv_range, resolve_json_array_range,
    resolve_map_path, resolve_ndjson_range, resolve_orc_range, resolve_parquet_range,
    resolve_psv_range, resolve_tsv_range, resolve_txt_range, RowGroupLocation,
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

fn create_sample_ndjson(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).unwrap();
    for value in 0..rows {
        writeln!(file, r#"{{"id":{},"value":{}}}"#, value, value * 10).unwrap();
    }
}

fn create_sample_json_array(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).unwrap();
    writeln!(file, "[").unwrap();
    for value in 0..rows {
        if value > 0 {
            writeln!(file, ",").unwrap();
        }
        if value == 65_535 {
            write!(
                file,
                r#"{{"id":{},"name":"brace {{ and }} and escaped \"quote\"","value":{}}}"#,
                value,
                value * 10
            )
            .unwrap();
        } else {
            write!(
                file,
                r#"{{"id":{},"name":"row{}","value":{}}}"#,
                value,
                value,
                value * 10
            )
            .unwrap();
        }
    }
    writeln!(file, "\n]").unwrap();
}

fn create_sample_arrow_ipc(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("scaled", DataType::Int64, false),
    ]));
    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();

    for start in [0i64, 3, 6] {
        let values = vec![start, start + 1, start + 2];
        let scaled = values.iter().map(|value| value * 10).collect::<Vec<_>>();
        writer
            .write(
                &RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(values)),
                        Arc::new(Int64Array::from(scaled)),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
}

fn create_multi_stripe_orc(path: &std::path::Path, rows: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(Int64Array::from_iter_values(
                (0..rows as i64).map(|value| value * 10),
            )),
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriterBuilder::new(file, schema)
        .with_batch_size(512)
        .with_stripe_byte_size(1024)
        .try_build()
        .unwrap();
    for start in (0..rows).step_by(512) {
        let length = 512.min(rows - start);
        writer.write(&batch.slice(start, length)).unwrap();
        writer.flush_stripe().unwrap();
    }
    writer.close().unwrap();
}

fn create_sample_csv(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).unwrap();
    writeln!(file, "id,name,value").unwrap();
    for value in 0..rows {
        if value == 65_535 {
            writeln!(file, "{},\"line one\nline two\",{}", value, value * 10).unwrap();
        } else {
            writeln!(file, "{},row{},{}", value, value, value * 10).unwrap();
        }
    }
}

fn create_sample_tsv(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).unwrap();
    writeln!(file, "id\tname\tvalue").unwrap();
    for value in 0..rows {
        if value == 65_535 {
            writeln!(file, "{}\t\"line one\nline two\"\t{}", value, value * 10).unwrap();
        } else {
            writeln!(file, "{}\trow{}\t{}", value, value, value * 10).unwrap();
        }
    }
}

fn create_sample_psv(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).unwrap();
    writeln!(file, "id|name|value").unwrap();
    for value in 0..rows {
        if value == 65_535 {
            writeln!(file, "{}|\"line one\nline two\"|{}", value, value * 10).unwrap();
        } else {
            writeln!(file, "{}|row{}|{}", value, value, value * 10).unwrap();
        }
    }
}

fn create_sample_txt(path: &std::path::Path, rows: usize) {
    let mut file = File::create(path).unwrap();
    writeln!(file, "id;name;value").unwrap();
    for value in 0..rows {
        if value == 65_535 {
            writeln!(file, "{};\"line one\nline two\";{}", value, value * 10).unwrap();
        } else {
            writeln!(file, "{};row{};{}", value, value, value * 10).unwrap();
        }
    }
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
    for extension in ["ipc", "arrow", "feather"] {
        let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
        let temp_dir = tempfile::tempdir().unwrap();
        let data_path = temp_dir.path().join(format!("data.{extension}"));
        create_sample_arrow_ipc(&data_path);

        engine
            .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
            .unwrap();

        let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
        assert_eq!(map.total_files, 1);
        assert_eq!(map.entries[0].rel_path, format!("data.{extension}"));
        assert_eq!(map.total_rows, 9);

        let row_groups: Vec<RowGroupLocation> =
            serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
        assert_eq!(row_groups.len(), 3, "{extension}");
        assert_eq!(row_groups[1].first_row, 3, "{extension}");
        assert!(row_groups.iter().all(|group| group.first_byte.is_none()));

        let resolved = resolve_arrow_ipc_range(&data_path, 4, 2).unwrap().unwrap();
        assert_eq!(resolved.batch_ordinal, 1, "{extension}");
        assert_eq!(resolved.offset, 1, "{extension}");

        let batch = engine
            .slice_rows_native(data_path.to_str().unwrap(), 4, 2)
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[4, 5], "{extension}");

        let batch = engine
            .slice_cols_native(data_path.to_str().unwrap(), &[String::from("scaled")], 4, 2)
            .unwrap();
        assert_eq!(batch.schema().fields()[0].name(), "scaled");
        let scaled = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(scaled.values(), &[40, 50], "{extension}");
    }
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

#[test]
fn test_lake_map_resolves_ndjson_byte_blocks_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.ndjson");
    create_sample_ndjson(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].first_row, 0);
    assert_eq!(row_groups[0].row_count, 65_536);
    assert!(row_groups[1].first_byte.unwrap() > row_groups[0].first_byte.unwrap());

    let resolved = resolve_ndjson_range(&file, 65_540, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 4);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 65_540, 2)
        .unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 65_540);
    assert_eq!(ids.value(1), 65_541);

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_540, 2)
        .unwrap();
    assert_eq!(batch.schema().fields()[0].name(), "value");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 655_400);
    assert_eq!(values.value(1), 655_410);

    let first = engine
        .slice_rows_native(file.to_str().unwrap(), 0, 1)
        .unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );

    let last = engine
        .slice_rows_native(file.to_str().unwrap(), 69_999, 2)
        .unwrap();
    assert_eq!(last.num_rows(), 1);
    assert_eq!(
        last.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        69_999
    );
}

#[test]
fn test_lake_map_resolves_jsonl_alias_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.jsonl");
    create_sample_ndjson(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].row_count, 65_536);

    let resolved = resolve_ndjson_range(&file, 65_536, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 0);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_536, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[655_360, 655_370]);
}

#[test]
fn test_lake_map_resolves_json_array_blocks_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.json");
    create_sample_json_array(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].row_count, 65_536);
    assert!(row_groups[1].first_byte.unwrap() > row_groups[0].first_byte.unwrap());

    let resolved = resolve_json_array_range(&file, 65_540, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 4);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 65_540, 2)
        .unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[65_540, 65_541]);

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_540, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[655_400, 655_410]);

    let special = engine
        .slice_rows_native(file.to_str().unwrap(), 65_535, 1)
        .unwrap();
    let name = special
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "brace { and } and escaped \"quote\"");
}

#[test]
fn test_lake_map_resolves_orc_stripes_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.orc");
    create_multi_stripe_orc(&file, 25_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert!(row_groups.len() > 1);
    assert_eq!(map.total_rows, 25_000);
    assert!(row_groups
        .iter()
        .all(|group| group.first_byte.is_some() && group.total_byte_size > 0));

    let group = &row_groups[1];
    let offset = group.first_row + 1;
    let resolved = resolve_orc_range(&file, offset, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 1);
    assert_eq!(resolved.byte_offset, group.first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), offset, 2)
        .unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[offset as i64, offset as i64 + 1]);

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], offset, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        values.values(),
        &[offset as i64 * 10, (offset as i64 + 1) * 10]
    );
}

#[test]
fn test_lake_map_resolves_csv_quote_safe_blocks_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.csv");
    create_sample_csv(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].row_count, 65_536);
    assert!(row_groups[1].first_byte.unwrap() > row_groups[0].first_byte.unwrap());

    let resolved = resolve_csv_range(&file, 65_536, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 0);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 65_536, 2)
        .unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[65_536, 65_537]);
    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "row65536");

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_536, 2)
        .unwrap();
    assert_eq!(batch.schema().fields()[0].name(), "value");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[655_360, 655_370]);
}

#[test]
fn test_lake_map_resolves_tsv_quote_safe_blocks_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.tsv");
    create_sample_tsv(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].row_count, 65_536);

    let resolved = resolve_tsv_range(&file, 65_536, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 0);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 65_536, 2)
        .unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "65536");
    assert_eq!(ids.value(1), "65537");

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_536, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "655360");
    assert_eq!(values.value(1), "655370");
}

#[test]
fn test_lake_map_resolves_psv_quote_safe_blocks_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.psv");
    create_sample_psv(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].row_count, 65_536);

    let resolved = resolve_psv_range(&file, 65_536, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 0);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 65_536, 2)
        .unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[65_536, 65_537]);

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_536, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[655_360, 655_370]);
}

#[test]
fn test_lake_map_resolves_txt_quote_safe_blocks_for_slice() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let file = temp_dir.path().join("mapped.txt");
    create_sample_txt(&file, 70_000);

    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    let row_groups: Vec<RowGroupLocation> =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.len(), 2);
    assert_eq!(row_groups[0].row_count, 65_536);

    let resolved = resolve_txt_range(&file, 65_536, 2).unwrap().unwrap();
    assert_eq!(resolved.offset, 0);
    assert_eq!(resolved.byte_offset, row_groups[1].first_byte.unwrap());

    let batch = engine
        .slice_rows_native(file.to_str().unwrap(), 65_536, 2)
        .unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[65_536, 65_537]);

    let batch = engine
        .slice_cols_native(file.to_str().unwrap(), &[String::from("value")], 65_536, 2)
        .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[655_360, 655_370]);
}
