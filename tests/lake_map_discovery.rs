use std::sync::Arc;

use basaltic_red::engine::formats::{register_format, unregister_format, DelimitedFormatHandler};
use basaltic_red::engine::map::{load_lake_map_ipc, resolve_map_path};
use basaltic_red::engine::MatrixEngine;
use basaltic_red::utils::discover_data_files;

#[test]
fn map_doctor_and_auto_heal_share_extension_discovery() -> anyhow::Result<()> {
    const DYNAMIC_EXT: &str = "lakecustom";
    register_format(
        DYNAMIC_EXT,
        Arc::new(DelimitedFormatHandler::new(b'|', true)),
    );

    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("part.csv"), "id,value\n1,10\n")?;
    std::fs::write(
        dir.path().join(format!("part.{DYNAMIC_EXT}")),
        "id|value\n2|20\n",
    )?;
    let extensionless = dir.path().join("raw-data");
    std::fs::write(&extensionless, "id,value\n3,30\n")?;

    let discovered = discover_data_files(dir.path(), None)?;
    let names: Vec<String> = discovered
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        names,
        vec![String::from("part.csv"), format!("part.{DYNAMIC_EXT}")]
    );

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    engine.create_lake_map_native(dir.path().to_str().unwrap(), false)?;
    let map = load_lake_map_ipc(&resolve_map_path(dir.path()))?;
    assert_eq!(map.total_files, 2);
    assert_eq!(map.total_rows, 2);
    assert_eq!(
        map.entries
            .iter()
            .find(|entry| entry.rel_path.ends_with(DYNAMIC_EXT))
            .unwrap()
            .row_groups_json,
        "[]"
    );

    let report = engine.doctor_lake_map_native(dir.path().to_str().unwrap(), false)?;
    assert_eq!(report.status, "HEALTHY");
    assert_eq!(report.total_files, 2);
    assert!(report.unindexed_files.is_empty());

    // Direct reads can sniff a format without an extension, but directory discovery omits it.
    let raw = engine.slice_rows_native(extensionless.to_str().unwrap(), 0, 10)?;
    assert_eq!(raw.num_rows(), 1);

    std::fs::write(
        dir.path().join(format!("new.{DYNAMIC_EXT}")),
        "id|value\n4|40\n",
    )?;
    std::fs::write(dir.path().join("new-raw"), "id,value\n5,50\n")?;
    let drift = engine.doctor_lake_map_native(dir.path().to_str().unwrap(), false)?;
    assert_eq!(drift.unindexed_files.len(), 1);
    assert!(drift.unindexed_files[0].ends_with(DYNAMIC_EXT));

    let healed = engine.doctor_lake_map_native(dir.path().to_str().unwrap(), true)?;
    assert_eq!(healed.status, "HEALED");
    let healthy = engine.doctor_lake_map_native(dir.path().to_str().unwrap(), false)?;
    assert_eq!(healthy.status, "HEALTHY");
    assert_eq!(healthy.total_files, 3);
    let map = load_lake_map_ipc(&resolve_map_path(dir.path()))?;
    assert_eq!(map.total_files, 3);
    assert_eq!(map.total_rows, 3);
    assert!(map
        .entries
        .iter()
        .all(|entry| !entry.rel_path.contains("raw")));

    assert!(unregister_format(DYNAMIC_EXT));
    Ok(())
}
