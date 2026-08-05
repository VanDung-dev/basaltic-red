use std::path::{Path, PathBuf};

use crate::error::BazanError;

pub fn discover_data_files(
    dir: &Path,
    filter_subfolder: Option<&str>,
) -> Result<Vec<PathBuf>, BazanError> {
    let mut files = Vec::new();
    if !dir.exists() || !dir.is_dir() {
        return Ok(files);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // entry.file_type() does not follow symlinks: symlinks (files or dirs)
        // are skipped so a planted link cannot read outside the input scope.
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            // Partition Pruning: Check if subfolder path contains filter pattern
            if let Some(filter) = filter_subfolder {
                let full_path_str = path.to_str().unwrap_or("");
                if !full_path_str.contains(filter) && !contains_subfolder_matching(&path, filter) {
                    continue; // Skip pruned partition branch
                }
            }
            files.extend(discover_data_files(&path, filter_subfolder)?);
        } else if file_type.is_file() {
            if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
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
                    if let Some(filter) = filter_subfolder {
                        let full_path_str = path.to_str().unwrap_or("");
                        if !full_path_str.contains(filter) {
                            continue;
                        }
                    }
                    files.push(path);
                }
            }
        }
    }

    Ok(files)
}

pub fn discover_parquet_files(
    dir: &Path,
    filter_subfolder: Option<&str>,
) -> Result<Vec<PathBuf>, BazanError> {
    discover_data_files(dir, filter_subfolder)
}

pub fn contains_subfolder_matching(dir: &Path, filter: &str) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            // Do not follow symlinks while probing partition names
            if entry.file_type().is_ok_and(|t| t.is_symlink()) {
                continue;
            }
            let path = entry.path();
            if path.to_str().is_some_and(|s| s.contains(filter)) {
                return true;
            }
            if path.is_dir() && contains_subfolder_matching(&path, filter) {
                return true;
            }
        }
    }
    false
}
