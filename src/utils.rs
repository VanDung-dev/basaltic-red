use std::path::{Path, PathBuf};

use crate::error::BazanError;

/// Validate that a given path stays within the allowed sandbox root, if configured.
///
/// If `BASALTIC_RED_DATA_ROOT` or `BASALTIC_RED_ALLOWED_ROOT` is set in the environment,
/// this checks that the path (canonicalized, or resolved relative to current dir)
/// resides within that root. If not configured, all paths are allowed.
pub fn validate_safe_path(path: &Path) -> Result<PathBuf, BazanError> {
    let allowed_root_var = std::env::var("BASALTIC_RED_DATA_ROOT")
        .or_else(|_| std::env::var("BASALTIC_RED_ALLOWED_ROOT"));

    if let Ok(root_str) = allowed_root_var {
        let trimmed = root_str.trim();
        if !trimmed.is_empty() {
            let root_path = Path::new(trimmed);
            let canonical_root = std::fs::canonicalize(root_path).unwrap_or_else(|_| {
                if root_path.is_absolute() {
                    root_path.to_path_buf()
                } else {
                    std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(root_path)
                }
            });

            let canonical_target = if path.exists() {
                std::fs::canonicalize(path)?
            } else if let Some(parent) = path.parent() {
                if parent.as_os_str().is_empty() {
                    std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(path)
                } else if parent.exists() {
                    let canon_parent = std::fs::canonicalize(parent)?;
                    canon_parent.join(path.file_name().unwrap_or_default())
                } else {
                    path.to_path_buf()
                }
            } else {
                path.to_path_buf()
            };

            if !canonical_target.starts_with(&canonical_root) {
                return Err(BazanError::Message(format!(
                    "Path traversal denied: path '{}' escapes allowed root '{}'",
                    path.display(),
                    canonical_root.display()
                )));
            }

            return Ok(canonical_target);
        }
    }

    Ok(path.to_path_buf())
}

pub fn discover_data_files(
    dir: &Path,
    filter_subfolder: Option<&str>,
) -> Result<Vec<PathBuf>, BazanError> {
    let safe_dir = validate_safe_path(dir)?;
    let dir = safe_dir.as_path();
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
