use std::path::Path;
use tempfile::tempdir;

use basaltic_red::engine::dynamic_filter::FilterRule;
use basaltic_red::engine::partition::{
    discover_and_prune_files, matches_partition_rules, parse_path_partitions,
};

#[test]
fn test_hive_partition_parsing() {
    let path = Path::new("data/lakehouse/year=2026/month=08/region=US/data.parquet");
    let parts = parse_path_partitions(path);

    assert_eq!(parts.get("year").unwrap(), "2026");
    assert_eq!(parts.get("month").unwrap(), "08");
    assert_eq!(parts.get("region").unwrap(), "US");
}

#[test]
fn test_partition_rule_pruning() -> anyhow::Result<()> {
    let path_match = Path::new("data/year=2026/month=08/data.parquet");
    let path_fail = Path::new("data/year=2025/month=08/data.parquet");

    let rules = vec![
        FilterRule::parse("year >= 2026")?,
        FilterRule::parse("month == '08'")?,
    ];

    let parts_match = parse_path_partitions(path_match);
    let parts_fail = parse_path_partitions(path_fail);

    assert!(matches_partition_rules(&parts_match, &rules));
    assert!(!matches_partition_rules(&parts_fail, &rules));

    Ok(())
}

#[test]
fn test_discover_and_prune_directory_tree() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let dir_2025 = dir.path().join("year=2025").join("month=08");
    let dir_2026 = dir.path().join("year=2026").join("month=08");

    std::fs::create_dir_all(&dir_2025)?;
    std::fs::create_dir_all(&dir_2026)?;

    std::fs::write(dir_2025.join("old.csv"), "id,val\n1,10\n")?;
    std::fs::write(dir_2026.join("new.csv"), "id,val\n2,20\n")?;

    let rules = vec![FilterRule::parse("year >= 2026")?];
    let (files, pruned_count) = discover_and_prune_files(dir.path(), &rules, None)?;

    assert_eq!(files.len(), 1);
    assert!(files[0].to_str().unwrap().contains("year=2026"));
    assert!(pruned_count >= 1);

    Ok(())
}
