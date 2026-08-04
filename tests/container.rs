use tempfile::tempdir;

use basaltic_red::engine::container::{read_bazan_entry_batch, read_bazan_manifest};
use basaltic_red::engine::MatrixEngine;

#[test]
fn test_bazan_container_pack_inspect_and_read() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let input_dir = dir.path().join("input_db");
    let output_bazan = dir.path().join("test_db.bazan");

    std::fs::create_dir_all(input_dir.join("users"))?;
    std::fs::create_dir_all(input_dir.join("orders"))?;

    std::fs::write(
        input_dir.join("users/users.csv"),
        "id,name,age\n1,Alice,30\n2,Bob,25\n",
    )?;
    std::fs::write(
        input_dir.join("orders/orders.csv"),
        "id,user_id,amount\n101,1,250.5\n102,2,100.0\n",
    )?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    // 1. Pack
    let (total_entries, total_size) = engine.pack_directory_to_bazan(&input_dir, &output_bazan)?;
    assert_eq!(total_entries, 2);
    assert!(total_size > 0);

    // 2. Read Manifest (Inspect)
    let manifest = read_bazan_manifest(&output_bazan)?;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.entries.len(), 2);

    // 3. Read Entry Batch Directly from .bazan file
    let users_entry = manifest
        .entries
        .iter()
        .find(|e| e.path.contains("users"))
        .unwrap();
    let users_batch = read_bazan_entry_batch(&output_bazan, users_entry)?;
    assert_eq!(users_batch.num_rows(), 2);

    Ok(())
}
