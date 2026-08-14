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
            files.extend(discover_data_files(&path, filter_subfolder)?);
        } else if file_type.is_file() {
            if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
                let ext_lower = ext.to_lowercase();
                let is_supported = crate::engine::formats::handler_for(&ext_lower).is_some();
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

    files.sort();
    Ok(files)
}

pub fn discover_parquet_files(
    dir: &Path,
    filter_subfolder: Option<&str>,
) -> Result<Vec<PathBuf>, BazanError> {
    discover_data_files(dir, filter_subfolder)
}
