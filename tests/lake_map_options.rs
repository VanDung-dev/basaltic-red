use std::fs::File;
use std::io::Write;
use std::time::UNIX_EPOCH;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::FileWriter;
use basaltic_red::engine::map::{
    doctor_lake_map, load_lake_map_ipc, resolve_map_path, save_lake_map_ipc, FingerprintPolicy,
    LakeMapOptions,
};
use basaltic_red::engine::MatrixEngine;

fn options(stride: usize, fingerprint: &str, stats: Option<Vec<String>>) -> LakeMapOptions {
    LakeMapOptions::new(stride, fingerprint, stats).unwrap()
}

fn write_rows(path: &std::path::Path, last_id: u8) {
    let mut file = File::create(path).unwrap();
    writeln!(file, "{{\"id\":1,\"label\":\"a\"}}").unwrap();
    writeln!(file, "{{\"id\":2,\"label\":\"b\"}}").unwrap();
    write!(file, "{{\"id\":{last_id},\"label\":\"c\"}}\n").unwrap();
}

#[test]
fn configured_map_persists_stride_fingerprint_and_selected_stats() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.ndjson");
    write_rows(&data_path, 3);

    engine
        .create_lake_map_native_with_options(
            temp_dir.path().to_str().unwrap(),
            false,
            options(2, "blake3", Some(vec!["id".to_string()])),
        )
        .unwrap();

    let map = load_lake_map_ipc(&resolve_map_path(temp_dir.path())).unwrap();
    assert_eq!(map.options.checkpoint_stride_rows.get(), 2);
    assert_eq!(map.options.fingerprint, FingerprintPolicy::Blake3);
    assert_eq!(map.options.stats_columns, Some(vec!["id".to_string()]));
    assert!(map.entries[0].content_hash.is_some());
    let schema = map.to_record_batch().unwrap().schema();
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        &names[..7],
        &[
            "rel_path",
            "size_bytes",
            "mtime_ms",
            "total_rows",
            "stats_json",
            "first_global_row",
            "row_groups_json"
        ][..]
    );
    assert_eq!(names[7], "content_hash");
    assert_eq!(schema.metadata().get("bazan.map_schema").unwrap(), "3");

    let stats: serde_json::Value = serde_json::from_str(&map.entries[0].stats_json).unwrap();
    assert_eq!(stats["total_rows"], 3);
    assert!(stats["columns"].get("id").is_some());
    assert!(stats["columns"].get("label").is_none());

    let row_groups: serde_json::Value =
        serde_json::from_str(&map.entries[0].row_groups_json).unwrap();
    assert_eq!(row_groups.as_array().unwrap().len(), 2);
    assert_eq!(row_groups[0]["row_count"], 2);
    assert_eq!(row_groups[1]["row_count"], 1);
}

#[test]
fn blake3_doctor_detects_same_metadata_edit_and_auto_heals_with_saved_options() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.ndjson");
    write_rows(&data_path, 3);
    let saved_options = options(1, "blake3", Some(Vec::new()));

    engine
        .create_lake_map_native_with_options(
            temp_dir.path().to_str().unwrap(),
            false,
            saved_options.clone(),
        )
        .unwrap();

    // Keep file size constant. Set the recorded mtime to the current value too,
    // simulating a same-size edit whose filesystem timestamp did not change.
    write_rows(&data_path, 9);
    let metadata = std::fs::metadata(&data_path).unwrap();
    let mtime_ms = metadata
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let map_path = resolve_map_path(temp_dir.path());
    let mut map = load_lake_map_ipc(&map_path).unwrap();
    map.entries[0].size_bytes = metadata.len();
    map.entries[0].mtime_ms = mtime_ms;
    save_lake_map_ipc(&map, &map_path).unwrap();

    let drift = doctor_lake_map(temp_dir.path(), false).unwrap();
    assert_eq!(drift.status, "DRIFT_DETECTED");
    assert_eq!(drift.modified_files, vec!["data.ndjson"]);

    let healed = doctor_lake_map(temp_dir.path(), true).unwrap();
    assert_eq!(healed.status, "HEALED");
    let healed_map = load_lake_map_ipc(&map_path).unwrap();
    assert_eq!(healed_map.options, saved_options);
    assert_eq!(
        healed_map.entries[0].content_hash,
        Some(
            blake3::hash(&std::fs::read(data_path).unwrap())
                .to_hex()
                .to_string()
        )
    );
    let stats: serde_json::Value = serde_json::from_str(&healed_map.entries[0].stats_json).unwrap();
    assert_eq!(stats["total_rows"], 3);
    assert_eq!(stats["columns"].as_object().unwrap().len(), 0);
    assert_eq!(
        doctor_lake_map(temp_dir.path(), false).unwrap().status,
        "HEALTHY"
    );
}

#[test]
fn schema_two_seven_column_map_loads_with_default_options() {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.ndjson");
    write_rows(&data_path, 3);
    engine
        .create_lake_map_native(temp_dir.path().to_str().unwrap(), false)
        .unwrap();

    let map_path = resolve_map_path(temp_dir.path());
    let map = load_lake_map_ipc(&map_path).unwrap();
    let current_batch = map.to_record_batch().unwrap();
    let fields: Vec<_> = current_batch
        .schema()
        .fields()
        .iter()
        .take(7)
        .map(|field| field.as_ref().clone())
        .collect();
    let mut metadata = current_batch.schema().metadata().clone();
    metadata.retain(|key, _| {
        !matches!(
            key.as_str(),
            "bazan.checkpoint_stride_rows" | "bazan.fingerprint" | "bazan.stats_columns"
        )
    });
    metadata.insert("bazan.map_schema".to_string(), "2".to_string());
    let schema = std::sync::Arc::new(Schema::new_with_metadata(fields, metadata));
    let arrays = current_batch.columns()[..7].to_vec();
    let legacy_batch = RecordBatch::try_new(schema, arrays).unwrap();
    let file = File::create(&map_path).unwrap();
    let legacy_schema = legacy_batch.schema();
    let mut writer = FileWriter::try_new(file, &legacy_schema).unwrap();
    writer.write(&legacy_batch).unwrap();
    writer.finish().unwrap();

    let loaded = load_lake_map_ipc(&map_path).unwrap();
    assert_eq!(loaded.options, LakeMapOptions::default());
    assert!(loaded.entries[0].content_hash.is_none());
}

#[test]
fn map_options_reject_zero_stride_and_unknown_fingerprint() {
    assert!(LakeMapOptions::new(0, "metadata", None).is_err());
    assert!(LakeMapOptions::new(1, "sha256", None).is_err());
}
