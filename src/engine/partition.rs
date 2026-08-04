use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::engine::dynamic_filter::{FilterRule, Operator};
use crate::error::BazanError;

/// Trích xuất các cặp partition key-value dạng Hive từ đường dẫn tập tin hoặc thư mục
/// Ví dụ: "data/year=2026/month=08/day=04/file.parquet"
/// -> {"year": "2026", "month": "08", "day": "04"}
pub fn parse_path_partitions(path: &Path) -> HashMap<String, String> {
    let mut partitions = HashMap::new();

    for component in path.components() {
        if let Some(segment) = component.as_os_str().to_str() {
            if let Some(eq_idx) = segment.find('=') {
                let key = segment[..eq_idx].trim().to_lowercase();
                let value = segment[eq_idx + 1..].trim().to_string();
                if !key.is_empty() && !value.is_empty() {
                    partitions.insert(key, value);
                }
            }
        }
    }

    partitions
}

/// Đánh giá các quy tắc lọc (FilterRules) trên các biến phân vùng (Partition key-values)
/// Trả về `true` nếu phân vùng THỎA MÃN (hoặc không chứa cột lọc đó).
/// Trả về `false` nếu phân vùng VI PHẠM điều kiện -> CẦN CẮT TỈA (PRUNE).
pub fn matches_partition_rules(partitions: &HashMap<String, String>, rules: &[FilterRule]) -> bool {
    for rule in rules {
        let rule_col_lower = rule.col_name.to_lowercase();

        if let Some(part_val_str) = partitions.get(&rule_col_lower) {
            // Thử so sánh dưới dạng số nguyên (Int64)
            if let (Ok(part_val_int), Ok(target_int)) = (part_val_str.parse::<i64>(), rule.val_str.parse::<i64>()) {
                if !eval_cmp(part_val_int, target_int, &rule.op) {
                    return false; // Vi phạm -> Prune
                }
                continue;
            }

            // Thử so sánh dưới dạng số thực (Float64)
            if let (Ok(part_val_float), Ok(target_float)) = (part_val_str.parse::<f64>(), rule.val_str.parse::<f64>()) {
                if !eval_cmp(part_val_float, target_float, &rule.op) {
                    return false; // Vi phạm -> Prune
                }
                continue;
            }

            // So sánh dưới dạng Chuỗi (String)
            if !eval_cmp(part_val_str.as_str(), rule.val_str.as_str(), &rule.op) {
                return false; // Vi phạm -> Prune
            }
        }
    }

    true
}

fn eval_cmp<T: PartialOrd>(val: T, target: T, op: &Operator) -> bool {
    match op {
        Operator::Gt => val > target,
        Operator::Gte => val >= target,
        Operator::Lt => val < target,
        Operator::Lte => val <= target,
        Operator::Eq => val == target,
        Operator::Neq => val != target,
    }
}

/// Duyệt cây thư mục đệ quy và cắt tỉa (prune) các nhánh thư mục vi phạm phân vùng
pub fn discover_and_prune_files(
    dir: &Path,
    rules: &[FilterRule],
    explicit_partition_filter: Option<&str>,
) -> Result<(Vec<PathBuf>, usize), BazanError> {
    let mut files = Vec::new();
    let mut pruned_dirs_count = 0;

    if !dir.exists() || !dir.is_dir() {
        return Ok((files, 0));
    }

    // Kiểm tra partition rules trực tiếp trên đường dẫn thư mục hiện tại
    let current_partitions = parse_path_partitions(dir);
    if !matches_partition_rules(&current_partitions, rules) {
        // Nhánh thư mục này không thỏa điều kiện -> Skip toàn bộ nhánh cây này!
        return Ok((files, 1));
    }

    // Kiểm tra bộ lọc từ khóa phân vùng trực tiếp (-p / --partition-filter)
    if let Some(filter_str) = explicit_partition_filter {
        let dir_str = dir.to_str().unwrap_or("");
        if !dir_str.contains(filter_str) && !crate::utils::contains_subfolder_matching(dir, filter_str) {
            return Ok((files, 1)); // Prune nhánh này
        }
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let (sub_files, sub_pruned) = discover_and_prune_files(&path, rules, explicit_partition_filter)?;
            files.extend(sub_files);
            pruned_dirs_count += sub_pruned;
        } else if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
            let ext_lower = ext.to_lowercase();
            let is_supported = matches!(
                ext_lower.as_str(),
                "parquet" | "pq" | "csv" | "tsv" | "psv" | "txt" | "json" | "ndjson" | "jsonl" | "feather" | "arrow" | "ipc" | "avro" | "xlsx" | "orc" | "msgpack"
            );

            if is_supported {
                // Kiểm tra lại partition rules trên full path của file
                let file_partitions = parse_path_partitions(&path);
                if !matches_partition_rules(&file_partitions, rules) {
                    continue; // Skip file này
                }

                if let Some(filter_str) = explicit_partition_filter {
                    let path_str = path.to_str().unwrap_or("");
                    if !path_str.contains(filter_str) {
                        continue;
                    }
                }

                files.push(path);
            }
        }
    }

    Ok((files, pruned_dirs_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
}
