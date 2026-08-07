use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::engine::dynamic_filter::{FilterRule, Operator};
use crate::error::BazanError;

/// Extract Hive-style partition key-value pairs from a file or directory path.
/// Example: "data/year=2026/month=08/day=04/file.parquet"
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

/// Evaluate filter rules (FilterRules) against partition variables (partition key-values).
/// Returns `true` if the partition SATISFIES the rules (or has none of the filter columns).
/// Returns `false` if the partition VIOLATES a condition -> needs PRUNING.
pub fn matches_partition_rules(partitions: &HashMap<String, String>, rules: &[FilterRule]) -> bool {
    for rule in rules {
        let rule_col_lower = rule.col_name.to_lowercase();

        if let Some(part_val_str) = partitions.get(&rule_col_lower) {
            // Try comparing as an integer (Int64)
            if let (Ok(part_val_int), Ok(target_int)) =
                (part_val_str.parse::<i64>(), rule.val_str.parse::<i64>())
            {
                if !eval_cmp(part_val_int, target_int, &rule.op) {
                    return false; // Violation -> Prune
                }
                continue;
            }

            // Try comparing as a float (Float64)
            if let (Ok(part_val_float), Ok(target_float)) =
                (part_val_str.parse::<f64>(), rule.val_str.parse::<f64>())
            {
                if !eval_cmp(part_val_float, target_float, &rule.op) {
                    return false; // Violation -> Prune
                }
                continue;
            }

            // Compare as a string (String)
            if !eval_cmp(part_val_str.as_str(), rule.val_str.as_str(), &rule.op) {
                return false; // Violation -> Prune
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

/// Recursively walk the directory tree and prune branches that violate partitions.
///
/// Each directory is visited exactly once (O(n)); a "dead" branch (does not match
/// the filter and has no matching descendant) is counted exactly once at the branch
/// root — not by re-scanning the whole subtree at every level like the old
/// `contains_subfolder_matching` did.
pub fn discover_and_prune_files(
    dir: &Path,
    rules: &[FilterRule],
    explicit_partition_filter: Option<&str>,
) -> Result<(Vec<PathBuf>, usize), BazanError> {
    let (files, _, pruned) = walk_and_prune(dir, rules, explicit_partition_filter)?;
    Ok((files, pruned))
}

/// Returns `(files, subtree_has_match, pruned_branch_count)`.
///
/// `subtree_has_match` is true when any path in this subtree contains the filter
/// (or when the subtree is fully alive because the filter is unset); `pruned` counts
/// only *maximal* dead branches — a dead parent supersedes its dead children — which
/// reproduces the old top-down counting without re-scanning.
fn walk_and_prune(
    dir: &Path,
    rules: &[FilterRule],
    filter: Option<&str>,
) -> Result<(Vec<PathBuf>, bool, usize), BazanError> {
    if !dir.exists() || !dir.is_dir() {
        return Ok((Vec::new(), false, 0));
    }

    // Rule-based pruning is O(1) (path parse) and prunes the whole branch at once.
    let current_partitions = parse_path_partitions(dir);
    if !matches_partition_rules(&current_partitions, rules) {
        return Ok((Vec::new(), false, 1));
    }

    let dir_matches = filter.map_or(true, |f| dir.to_str().unwrap_or("").contains(f));
    let mut files = Vec::new();
    let mut any_match = false;
    let mut pruned = 0usize;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // entry.file_type() does not follow symlinks: a planted link must not
        // pull files from outside the input scope.
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();

        if file_type.is_dir() {
            let (sub_files, sub_any, sub_pruned) = walk_and_prune(&path, rules, filter)?;
            pruned += sub_pruned;
            if sub_any {
                any_match = true;
                files.extend(sub_files);
            }
        } else if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
            let ext_lower = ext.to_lowercase();
            let is_supported = matches!(
                ext_lower.as_str(),
                "parquet"
                    | "pq"
                    | "csv"
                    | "tsv"
                    | "psv"
                    | "txt"
                    | "json"
                    | "ndjson"
                    | "jsonl"
                    | "feather"
                    | "arrow"
                    | "ipc"
                    | "avro"
                    | "xlsx"
                    | "orc"
                    | "msgpack"
            );

            if is_supported {
                // Re-check partition rules against the file's full path
                let file_partitions = parse_path_partitions(&path);
                if !matches_partition_rules(&file_partitions, rules) {
                    continue;
                }

                if let Some(filter_str) = filter {
                    let path_str = path.to_str().unwrap_or("");
                    if !path_str.contains(filter_str) {
                        continue;
                    }
                }

                files.push(path);
                any_match = true;
            }
        }
    }

    if !dir_matches && !any_match {
        // Whole branch is dead: count once as the maximal pruned root.
        Ok((Vec::new(), false, 1))
    } else {
        Ok((files, true, pruned))
    }
}
