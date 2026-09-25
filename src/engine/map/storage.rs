use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use rayon::prelude::*;

use crate::error::BazanError;
use crate::utils::discover_data_files;

use super::{
    LakeMap, LakeMapEntry, LakeMapOptions, MapProgressTracker, DEFAULT_MAP_FILENAME,
    LEGACY_MAP_FILENAME,
};
use super::inspect_file_entry;

/// Build full LakeMap for a directory in parallel using Rayon with live progress bar
pub fn build_lake_map(dir_path: &Path) -> Result<LakeMap, BazanError> {
    build_lake_map_with_progress(dir_path, true)
}

/// Build full LakeMap for a directory with configurable live progress bar
pub fn build_lake_map_with_progress(
    dir_path: &Path,
    show_progress: bool,
) -> Result<LakeMap, BazanError> {
    build_lake_map_with_options(dir_path, show_progress, LakeMapOptions::default())
}

pub fn build_lake_map_with_options(
    dir_path: &Path,
    show_progress: bool,
    options: LakeMapOptions,
) -> Result<LakeMap, BazanError> {
    if !dir_path.exists() || !dir_path.is_dir() {
        return Err(BazanError::Message(format!(
            "Directory does not exist: {:?}",
            dir_path
        )));
    }

    let files = discover_data_files(dir_path, None)?;
    if files.is_empty() {
        return Ok(LakeMap::new_with_options(Vec::new(), options));
    }

    // Filter out existing map file itself and collect initial file sizes
    let valid_files_with_size: Vec<(PathBuf, u64)> = files
        .into_iter()
        .filter(|p| !is_map_sidecar(p))
        .map(|p| {
            let size = fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            (p, size)
        })
        .collect();

    let total_files = valid_files_with_size.len();
    let total_bytes: u64 = valid_files_with_size.iter().map(|(_, s)| *s).sum();

    let tracker = Arc::new(MapProgressTracker::new(
        total_files,
        total_bytes,
        show_progress,
    ));

    let entries: Result<Vec<LakeMapEntry>, BazanError> = valid_files_with_size
        .par_iter()
        .map(|(file, size)| {
            let res = inspect_file_entry(dir_path, file, &options);
            tracker.inc(*size);
            res
        })
        .collect();

    Ok(LakeMap::new_with_options(entries?, options))
}

/// Save LakeMap to Arrow IPC payload format (`.br_map.bazan`).
/// Uses atomic write-to-temp-and-rename to prevent corrupting open mmaps (avoiding SIGBUS)
pub fn save_lake_map_ipc(map: &LakeMap, output_path: &Path) -> Result<(), BazanError> {
    let output_path = crate::utils::validate_safe_path(output_path)?;
    let output_path = output_path.as_path();

    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let batch = map.to_record_batch()?;

    // Write to a unique temporary file in the same directory for atomic rename
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_file_name = format!(
        ".{}.tmp.{}_{}",
        output_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("br_map"),
        pid,
        nanos
    );
    let tmp_path = parent.join(tmp_file_name);

    let write_res = (|| -> Result<(), BazanError> {
        let file = File::create(&tmp_path)?;
        let mut writer = FileWriter::try_new(file, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        Ok(())
    })();

    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    // Atomic rename replaces directory entry without truncating active mmaps
    if let Err(e) = fs::rename(&tmp_path, output_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(BazanError::Io(e));
    }

    Ok(())
}

/// Load LakeMap from an Arrow IPC binary file using memory-mapped zero-copy I/O in < 0.05ms
pub fn load_lake_map_ipc(input_path: &Path) -> Result<LakeMap, BazanError> {
    let input_path = crate::utils::validate_safe_path(input_path)?;
    let file = File::open(&input_path)?;
    // Use OS memory-mapping for instant, zero-syscall virtual memory access
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let cursor = std::io::Cursor::new(mmap);
    let reader = FileReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch_res in reader {
        batches.push(batch_res?);
    }

    if batches.is_empty() {
        return Ok(LakeMap::new(Vec::new()));
    }

    let schema = batches[0].schema();
    let unified_batch = arrow::compute::concat_batches(&schema, &batches)?;
    LakeMap::from_record_batch(&unified_batch)
}

/// Resolve the current map path used for writes: `dir/.br_map.bazan`.
pub fn resolve_map_path(dir_path: &Path) -> PathBuf {
    dir_path.join(DEFAULT_MAP_FILENAME)
}

pub(super) fn is_map_sidecar(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(DEFAULT_MAP_FILENAME | LEGACY_MAP_FILENAME)
    )
}

/// Find the nearest healthy map entry for a data file.
pub(super) fn resolve_healthy_map_entry(
    file_path: &Path,
) -> Result<Option<LakeMapEntry>, BazanError> {
    let Some(mut map_root) = file_path.parent() else {
        return Ok(None);
    };

    loop {
        let map_path = resolve_map_path(map_root);
        if map_path.is_file() {
            let map = load_lake_map_ipc(&map_path)?;
            let Some(rel_path) = file_path.strip_prefix(map_root).ok() else {
                return Ok(None);
            };
            let rel_path = rel_path.to_string_lossy();
            let Some(entry) = map.entries.iter().find(|entry| entry.rel_path == rel_path) else {
                return Ok(None);
            };

            let metadata = fs::metadata(file_path)?;
            let mtime_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or(0);
            if metadata.len() != entry.size_bytes || mtime_ms != entry.mtime_ms {
                return Ok(None);
            }

            return Ok(Some(entry.clone()));
        }

        let Some(parent) = map_root.parent() else {
            break;
        };
        if parent == map_root {
            break;
        }
        map_root = parent;
    }

    Ok(None)
}
