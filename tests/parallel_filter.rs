use tempfile::tempdir;

use basaltic_red::engine::dynamic_filter::FilterRule;
use basaltic_red::engine::MatrixEngine;

#[test]
fn test_parallel_file_filter() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let file1_path = dir.path().join("part1.csv");
    let file2_path = dir.path().join("part2.csv");

    std::fs::write(&file1_path, "id,age,salary\n1,25,1000\n2,15,500\n")?;
    std::fs::write(&file2_path, "id,age,salary\n3,30,1200\n4,17,400\n")?;

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let rules = vec![FilterRule::parse("age >= 18")?];

    let summary =
        engine.filter_files_parallel(dir.path().to_str().unwrap(), &rules, None, Some(2))?;

    assert_eq!(summary.total_files, 2);
    assert_eq!(summary.total_rows, 4);
    assert_eq!(summary.clean_rows, 2);
    assert_eq!(summary.trash_rows, 2);

    Ok(())
}
