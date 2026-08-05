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

#[tokio::test]
async fn test_sql_multi_table_join_on_bazan_container() -> Result<()> {
    let dir = tempdir()?;
    let input_dir = dir.path().join("relational_db");
    let output_bazan = dir.path().join("multi_table.bazan");

    std::fs::create_dir_all(&input_dir)?;
    std::fs::write(
        input_dir.join("users.csv"),
        "id,name\n1,Alice\n2,Bob\n",
    )?;
    std::fs::write(
        input_dir.join("orders.csv"),
        "order_id,user_id,amount\n101,1,50.0\n102,2,75.0\n103,1,20.0\n",
    )?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    engine.pack_directory_to_bazan(&input_dir, &output_bazan)?;

    let sql = format!(
        "SELECT users.name, SUM(orders.amount) as total_spent FROM '{}' JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY total_spent DESC",
        output_bazan.display()
    );
    let result = engine.execute_sql(&sql).await?;

    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.num_columns(), 2);

    Ok(())
}
