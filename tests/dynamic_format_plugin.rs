use std::sync::Arc;
use tempfile::tempdir;

use basaltic_red::engine::dynamic_filter::FilterRule;
use basaltic_red::engine::formats::{
    list_supported_formats, register_format, unregister_format, DelimitedFormatHandler,
};
use basaltic_red::engine::map::{load_lake_map_ipc, resolve_map_path};
use basaltic_red::engine::MatrixEngine;

#[tokio::test]
async fn test_dynamic_format_registration_and_query() -> anyhow::Result<()> {
    // 1. Check supported formats initially
    let initial_formats = list_supported_formats();
    assert!(initial_formats.contains(&"parquet".to_string()));
    assert!(initial_formats.contains(&"csv".to_string()));
    assert!(!initial_formats.contains(&"custom_pipe".to_string()));

    // 2. Register custom format plugin with delimiter `|`
    let pipe_handler = Arc::new(DelimitedFormatHandler::new(b'|', true));
    register_format("custom_pipe", pipe_handler);

    // Verify it is listed
    let updated_formats = list_supported_formats();
    assert!(updated_formats.contains(&"custom_pipe".to_string()));

    // 3. Create a custom .custom_pipe file
    let dir = tempdir()?;
    let file_path = dir.path().join("data.custom_pipe");
    let content = "id|name|age|salary\n1|Alice|28|95000\n2|Bob|17|20000\n3|Charlie|35|120000\n";
    std::fs::write(&file_path, content)?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    // 4. Test slice_rows_native
    let slice_batch = engine.slice_rows_native(file_path.to_str().unwrap(), 0, 2)?;
    assert_eq!(slice_batch.num_rows(), 2);
    assert_eq!(slice_batch.num_columns(), 4);

    // 5. Test parallel filter with rules
    let rules = vec![FilterRule::parse("age >= 18")?];
    let summary =
        engine.filter_files_parallel_native(dir.path().to_str().unwrap(), &rules, None, Some(1))?;
    assert_eq!(summary.total_files, 1);
    assert_eq!(summary.total_rows, 3);
    assert_eq!(summary.clean_rows, 2); // Alice, Charlie
    assert_eq!(summary.trash_rows, 1); // Bob

    // 6. Test DataFusion SQL over the custom format
    let sql_query = format!(
        "SELECT name, salary FROM '{}' WHERE age >= 18 ORDER BY salary DESC",
        file_path.to_str().unwrap()
    );
    let result = engine.execute_sql(&sql_query).await?;
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.num_columns(), 2);

    // 7. Unregister format
    assert!(unregister_format("custom_pipe"));
    assert!(!list_supported_formats().contains(&"custom_pipe".to_string()));

    Ok(())
}

#[test]
fn test_custom_tilde_delimited_plugin() -> anyhow::Result<()> {
    let tilde_handler = Arc::new(DelimitedFormatHandler::new(b'~', true));
    register_format("tilde", tilde_handler);

    let dir = tempdir()?;
    let file_path = dir.path().join("events.tilde");
    std::fs::write(
        &file_path,
        "event_id~user~status\n101~john~SUCCESS\n102~mary~FAILED\n",
    )?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let slice = engine.slice_rows_native(file_path.to_str().unwrap(), 0, 10)?;
    assert_eq!(slice.num_rows(), 2);
    assert_eq!(slice.num_columns(), 3);

    unregister_format("tilde");
    Ok(())
}

#[test]
fn test_dynamic_override_keeps_builtin_extension_map_slices_consistent() -> anyhow::Result<()> {
    register_format("csv", Arc::new(DelimitedFormatHandler::new(b'~', true)));

    let dir = tempdir()?;
    let file_path = dir.path().join("override.csv");
    std::fs::write(&file_path, "id~value\n1~10\n2~20\n3~30\n")?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let streamed_rows = engine.slice_rows_native(file_path.to_str().unwrap(), 1, 2)?;
    assert_eq!(streamed_rows.num_rows(), 2);
    assert_eq!(streamed_rows.schema().fields()[0].name(), "id");
    let streamed_values =
        engine.slice_cols_native(file_path.to_str().unwrap(), &[String::from("value")], 1, 2)?;
    assert_eq!(
        streamed_values
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .values(),
        &[20, 30]
    );

    engine.create_lake_map_native(dir.path().to_str().unwrap(), false)?;
    let map = load_lake_map_ipc(&resolve_map_path(dir.path()))?;
    assert_eq!(map.total_rows, 3);
    assert_eq!(map.entries[0].row_groups_json, "[]");

    let rows = engine.slice_rows_native(file_path.to_str().unwrap(), 1, 2)?;
    assert_eq!(rows.num_rows(), 2);
    assert_eq!(rows.schema().fields()[0].name(), "id");

    let selected =
        engine.slice_cols_native(file_path.to_str().unwrap(), &[String::from("value")], 1, 2)?;
    assert_eq!(selected.num_columns(), 1);
    assert_eq!(selected.schema().fields()[0].name(), "value");
    assert_eq!(
        selected
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .values(),
        &[20, 30]
    );

    assert!(unregister_format("csv"));
    Ok(())
}
