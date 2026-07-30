use std::path::{Path, PathBuf};

pub fn discover_data_files(dir: &Path, filter_subfolder: Option<&str>) -> Result<Vec<PathBuf>, anyhow::Error> {
    let mut files = Vec::new();
    if !dir.exists() || !dir.is_dir() {
        return Ok(files);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Partition Pruning: Check if subfolder path contains filter pattern
            if let Some(filter) = filter_subfolder {
                let full_path_str = path.to_str().unwrap_or("");
                if !full_path_str.contains(filter) && !contains_subfolder_matching(&path, filter) {
                    continue; // Skip pruned partition branch
                }
            }
            files.extend(discover_data_files(&path, filter_subfolder)?);
        } else if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
            let ext_lower = ext.to_lowercase();
            if ext_lower == "parquet" || ext_lower == "pq" || ext_lower == "csv" || ext_lower == "tsv" || ext_lower == "json" || ext_lower == "ndjson" || ext_lower == "jsonl" {
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

    Ok(files)
}

pub fn discover_parquet_files(dir: &Path, filter_subfolder: Option<&str>) -> Result<Vec<PathBuf>, anyhow::Error> {
    discover_data_files(dir, filter_subfolder)
}

pub fn contains_subfolder_matching(dir: &Path, filter: &str) -> bool {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.to_str().map_or(false, |s| s.contains(filter)) {
                return true;
            }
            if path.is_dir() && contains_subfolder_matching(&path, filter) {
                return true;
            }
        }
    }
    false
}
