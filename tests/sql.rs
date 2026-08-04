use anyhow::Result;
use tempfile::tempdir;

use basaltic_red::engine::MatrixEngine;

#[tokio::test]
async fn test_sql_query_on_bazan_container() -> Result<()> {
    let dir = tempdir()?;
    let input_dir = dir.path().join("input_db");
    let output_bazan = dir.path().join("test_sql.bazan");

    std::fs::create_dir_all(&input_dir)?;
    std::fs::write(
        input_dir.join("data.csv"),
        "id,age,salary\n1,25,1000\n2,15,500\n3,30,1200\n",
    )?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    engine.pack_directory_to_bazan(&input_dir, &output_bazan)?;

    let sql = format!(
        "SELECT id, salary FROM '{}' WHERE age >= 18 ORDER BY salary DESC",
        output_bazan.display()
    );
    let result = engine.execute_sql(&sql).await?;

    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.num_columns(), 2);

    Ok(())
}
